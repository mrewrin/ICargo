import logging
import asyncio
from config import bitrix  # Используем инициализированный BitrixAsync из config
from db_management import get_unprocessed_webhooks, mark_webhook_as_processed
from process_functions import process_contact_update, process_deal_add

# Инициализация логирования
logging.basicConfig(level=logging.INFO)
logging.getLogger('fast_bitrix24').addHandler(logging.StreamHandler())


# ========== Пакетное получение информации из Bitrix ==========

async def fetch_batch_entity_info(entity_ids, entity_type):
    """
    Получает информацию о сущностях (сделках или контактах) из Bitrix в пакетном режиме,
    используя метод get_by_ID для более удобной обработки данных.

    :param entity_ids: Список идентификаторов сущностей.
    :param entity_type: Тип сущности - 'deal' или 'contact'.
    :return: Список словарей с информацией о сущностях.
    """
    if not entity_ids:
        return []

    operations = {}
    for idx, entity_id in enumerate(entity_ids):
        operation_key = f"{entity_type}_{idx}"
        if entity_type == 'deal':
            operations[operation_key] = f"crm.deal.get?ID={entity_id}"
        elif entity_type == 'contact':
            operations[operation_key] = f"crm.contact.get?ID={entity_id}"

    try:
        response = await bitrix.call_batch({
            'halt': 0,
            'cmd': operations
        })

        # Проверяем, что response содержит сразу нужные данные сделок
        if isinstance(response, dict):
            all_entity_info = [entity_data for entity_data in response.values()]
            logging.info(f"Получено {len(all_entity_info)} записей для типа {entity_type}")
            return all_entity_info
        else:
            logging.error(f"Непредвиденная структура ответа: {response}")
            return []

    except Exception as e:
        logging.error(f"Ошибка при выполнении batch запроса: {e}")
        return []


# ========== Пакетная отправка данных в Bitrix ==========

# Функция разделения операций на чанки
def chunk_operations(operations, batch_size=50):
    """
    Разделяет словарь операций на чанки.
    :param operations: Словарь операций.
    :param batch_size: Размер чанка.
    :return: Итератор с частями операций.
    """
    operations_list = list(operations.items())  # Преобразуем в список для индексирования
    for i in range(0, len(operations_list), batch_size):
        yield dict(operations_list[i:i + batch_size])


# Основная функция пакетной отправки
async def batch_send_to_bitrix():
    """
    Получает необработанные вебхуки, извлекает данные из Bitrix и отправляет их на дальнейшую обработку.
    """
    logging.info("Запуск пакетной обработки и отправки в Bitrix...")
    webhooks = get_unprocessed_webhooks()
    if not webhooks:
        logging.info("Нет необработанных вебхуков для отправки в Bitrix.")
        return

    logging.info(f"Количество вебхуков для обработки: {len(webhooks)}")
    await asyncio.sleep(2)  # Задержка в 2 секунды для стабилизации

    # Разделение вебхуков по типу событий
    deal_add_ids = [wh['entity_id'] for wh in webhooks if wh['event_type'] == 'ONCRMDEALADD']
    contact_update_ids = [wh['entity_id'] for wh in webhooks if wh['event_type'] == 'ONCRMCONTACTUPDATE']

    # Получаем информацию по сделкам и контактам
    deal_adds_info = []
    contact_updates_info = []
    if deal_add_ids:
        deal_adds_info = await fetch_batch_entity_info(deal_add_ids, entity_type="deal")
    if contact_update_ids:
        contact_updates_info = await fetch_batch_entity_info(contact_update_ids, entity_type="contact")

    # Инициализируем общий operations для всех сделок
    operations = {}

    # Обрабатываем добавление и обновление сделок, а также обновление контактов
    for deal_info in deal_adds_info:
        await process_deal_add(deal_info, operations)
    for contact_info in contact_updates_info:
        await process_contact_update(contact_info)

    # Финальный блок отправки batch-запроса после всех операций
    if operations:
        try:
            # Разделяем операции на чанки
            for batch_chunk in chunk_operations(operations, batch_size=50):
                batch_cmd = {'halt': 0, 'cmd': batch_chunk}
                logging.info(f"Отправка операций в batch: {batch_cmd}")

                # Выполняем batch-запрос
                response = await bitrix.call_batch(batch_cmd)

                # Проверка ответа
                if 'error' in response:
                    logging.error(f"Ошибка в batch: {response['error']}")
                    partial_results = response.get('result', {})
                    if partial_results:
                        logging.warning(f"Частично выполненные операции: {partial_results}")
                else:
                    logging.info(f"Batch операции успешно выполнены: {response}")

        except Exception as e:
            logging.exception(f"Неожиданная ошибка при отправке batch-запросов: {e}")

    else:
        logging.warning("Нет операций для выполнения в batch.")

    # Отмечаем вебхуки как обработанные
    for webhook in webhooks:
        try:
            mark_webhook_as_processed(webhook['id'])  # Обозначаем каждый вебхук как обработанный
        except Exception as e:
            logging.error(f"Ошибка при обработке вебхука {webhook['id']}: {e}")

    logging.info("Пакетная отправка данных в Bitrix завершена.")
