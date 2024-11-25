# import logging
# import asyncio
# from config import bitrix  # Используем инициализированный BitrixAsync из config
# from db_management import get_unprocessed_webhooks, mark_webhook_as_processed
# from process_functions import process_contact_update, process_deal_add
#
# # Инициализация логирования
# logging.basicConfig(
#     level=logging.DEBUG,  # Устанавливаем минимальный уровень логирования
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
# logging.getLogger('fast_bitrix24').addHandler(logging.StreamHandler())
#
#
# # ========== Пакетное получение информации из Bitrix ==========
#
# async def fetch_batch_entity_info(entity_ids, entity_type):
#     """
#     Получает информацию о сущностях (сделках или контактах) из Bitrix в пакетном режиме,
#     используя метод get_by_ID для более удобной обработки данных.
#
#     :param entity_ids: Список идентификаторов сущностей.
#     :param entity_type: Тип сущности - 'deal' или 'contact'.
#     :return: Список словарей с информацией о сущностях.
#     """
#     if not entity_ids:
#         return []
#
#     operations = {}
#     for idx, entity_id in enumerate(entity_ids):
#         operation_key = f"{entity_type}_{idx}"
#         if entity_type == 'deal':
#             operations[operation_key] = f"crm.deal.get?ID={entity_id}"
#         elif entity_type == 'contact':
#             operations[operation_key] = f"crm.contact.get?ID={entity_id}"
#
#     try:
#         response = await bitrix.call_batch({
#             'halt': 0,
#             'cmd': operations
#         })
#
#         # Проверяем, что response содержит сразу нужные данные сделок
#         if isinstance(response, dict):
#             all_entity_info = [entity_data for entity_data in response.values()]
#             logging.info(f"Получено {len(all_entity_info)} записей для типа {entity_type}")
#             return all_entity_info
#         else:
#             logging.error(f"Непредвиденная структура ответа: {response}")
#             return []
#
#     except Exception as e:
#         logging.error(f"Ошибка при выполнении batch запроса: {e}")
#         return []
#
#
# # ========== Пакетная отправка данных в Bitrix ==========
#
# # Функция разделения операций на чанки (без дублирования)
# def chunk_operations(operations, batch_size):
#     operations_list = list(operations.items())
#     for i in range(0, len(operations_list), batch_size):
#         yield dict(operations_list[i:i + batch_size])
#
#
# async def send_batch_chunk(batch_chunk, batch_size=50, max_retries=5):
#     """
#     Отправляет batch-запрос в Bitrix и возвращает результат выполнения.
#     """
#     retry_count = 0
#     while retry_count < max_retries:
#         try:
#             # Формируем batch-команду
#             batch_cmd = {'halt': 0, 'cmd': batch_chunk}
#             logging.info(f"Отправка batch-запроса: {list(batch_chunk.keys())}")
#
#             # Выполняем batch-запрос
#             response = await bitrix.call_batch(batch_cmd)
#
#             # Обработка ошибок
#             if 'error' in response:
#                 error_type = response.get('error')
#                 if error_type == 'ERROR_BATCH_LENGTH_EXCEEDED' and batch_size > 1:
#                     logging.warning("Превышен лимит batch-запроса. Разделяем.")
#                     results = {}
#                     for smaller_chunk in chunk_operations(batch_chunk, batch_size // 2):
#                         # Рекурсивно обрабатываем мелкие чанки
#                         chunk_result = await send_batch_chunk(smaller_chunk, batch_size // 2, max_retries)
#                         results.update(chunk_result)  # Собираем результаты
#                     logging.debug(f"Обработка batch-запроса завершена. Результат: {response}")
#                     return results
#                 else:
#                     logging.error(f"Ошибка в batch-запросе: {response['error']}")
#                     return {}
#
#             # Логируем успешный ответ
#             logging.info("Batch успешно выполнен.")
#             logging.debug(f"Результаты batch-запроса: {response}")
#             return response  # Возвращаем результат
#         except Exception as e:
#             retry_count += 1
#             logging.error(f"Ошибка в batch-запросе. Попытка {retry_count}: {e}")
#             await asyncio.sleep(2)
#
#     # Если после всех попыток запрос не удался
#     logging.error(f"Не удалось обработать чанк после {max_retries} попыток: {list(batch_chunk.keys())}")
#     return {}
#
#
# async def batch_send_to_bitrix():
#     """
#     Получает необработанные вебхуки, извлекает данные из Bitrix и отправляет их на дальнейшую обработку.
#     """
#     logging.info("Запуск пакетной обработки.")
#     webhooks = get_unprocessed_webhooks()
#     if not webhooks:
#         logging.info("Нет необработанных вебхуков.")
#         return
#
#     logging.info(f"Обработка {len(webhooks)} вебхуков.")
#     await asyncio.sleep(2)  # Задержка для стабилизации
#
#     operations = {}
#     unregistered_deals = []  # Для хранения сделок с незарегистрированными трек-номерами
#
#     for webhook in webhooks:
#         entity_id = webhook['entity_id']
#         event_type = webhook['event_type']
#         try:
#             if event_type == "ONCRMDEALADD":
#                 deal_info = await fetch_batch_entity_info([entity_id], "deal")
#                 if deal_info:
#                     await process_deal_add(deal_info[0], operations, unregistered_deals)
#             elif event_type == "ONCRMCONTACTUPDATE":
#                 contact_info = await fetch_batch_entity_info([entity_id], "contact")
#                 if contact_info:
#                     await process_contact_update(contact_info[0])
#         except Exception as e:
#             logging.error(f"Ошибка при обработке вебхука {webhook}: {e}")
#
#     # Обработка незарегистрированных трек-номеров
#     await handle_unregistered_deals(unregistered_deals, operations)
#
#     # Финальная отправка batch-запросов
#     if operations:
#         for batch_chunk in chunk_operations(operations, batch_size=50):
#             await send_batch_chunk(batch_chunk)
#             asyncio.sleep(1)
#     else:
#         logging.warning("Нет операций для batch-запроса.")
#
#     # Отмечаем вебхуки как обработанные
#     for webhook in webhooks:
#         try:
#             mark_webhook_as_processed(webhook['id'])  # Обозначаем каждый вебхук как обработанный
#             logging.info(f"Вебхук {webhook['id']} успешно обработан.")
#         except Exception as e:
#             logging.error(f"Ошибка при обработке вебхука {webhook['id']}: {e}")
#
#
# async def handle_unregistered_deals(unregistered_deals, operations):
#     if not unregistered_deals:
#         logging.info("Нет сделок без зарегистрированных трек-номеров для обработки.")
#         return
#
#     logging.info(f"Начата обработка {len(unregistered_deals)} сделок без зарегистрированных трек-номеров.")
#
#     # Шаг 1: Формируем запросы для поиска дубликатов
#     search_operations = {}
#     for idx, deal in enumerate(unregistered_deals):
#         track_number = deal.get('track_number')
#         if not track_number:
#             logging.warning(f"Пропущена сделка ID: {deal['ID']} из-за отсутствия трек-номера.")
#             continue
#
#         search_operations[f"search_{idx}"] = (
#             f"crm.deal.list?"
#             f"filter[UF_CRM_1723542556619]={track_number}&"
#             f"filter[!ID]={deal['ID']}&"
#             f"select[]=ID&"
#             f"select[]=STAGE_ID"
#         )
#         logging.debug(f"Добавлена операция поиска: search_{idx} для трек-номера {track_number}")
#
#     logging.info(f"Сформировано {len(search_operations)} операций для поиска дубликатов.")
#
#     # Шаг 2: Отправляем запросы чанками
#     duplicate_results = {}
#     for batch_chunk in chunk_operations(search_operations, batch_size=50):
#         logging.debug(f"Отправляем следующий batch-чанк: {batch_chunk}")
#         chunk_result = await send_batch_chunk(batch_chunk)
#         logging.debug(f"Результаты batch-чанка: {chunk_result}")
#         if chunk_result:
#             duplicate_results.update(chunk_result)
#
#     # Проверяем, не потерялись ли данные
#     logging.debug(f"Итоговые результаты дубликатов: {duplicate_results}")
#
#     # Шаг 3: Обрабатываем результаты поиска
#     duplicate_ids = set()  # Используем set для исключения повторений
#
#     for idx, deal in enumerate(unregistered_deals):
#         deal_stage_id = deal.get('STAGE_ID')
#         deal_id = deal.get('ID')
#         logging.debug(f"Обрабатывается сделка ID={deal_id}, STAGE_ID={deal_stage_id}")
#
#         # Проходим по результатам поиска
#         for key, result_list in duplicate_results.items():
#             if not isinstance(result_list, list):
#                 logging.warning(f"Ожидался список, но получено {type(result_list)} для ключа {key}. Пропускаем.")
#                 continue
#
#             # Обрабатываем список дубликатов
#             for duplicate in result_list:
#                 duplicate_stage_id = duplicate.get('STAGE_ID')
#                 duplicate_id = duplicate.get('ID')
#
#                 if duplicate_id != deal_id and duplicate_stage_id != deal_stage_id:
#                     logging.debug(f"Добавлен дубликат: ID={duplicate_id}, STAGE_ID={duplicate_stage_id}")
#                     duplicate_ids.add(duplicate_id)
#                 else:
#                     logging.debug(f"Исключён из дубликатов: ID={duplicate_id}, STAGE_ID={duplicate_stage_id}")
#
#     # Проверяем, есть ли дубликаты
#     if not duplicate_ids:
#         logging.info("Дубликаты не найдены. Обработка завершена.")
#         return
#
#     # Преобразуем обратно в список для формирования операций
#     duplicate_ids = list(duplicate_ids)
#
#     logging.info(f"Найдено {len(duplicate_ids)} уникальных дубликатов. Добавляем операции на удаление.")
#
#     # Шаг 4: Добавляем операции на удаление
#     for idx, deal_id in enumerate(duplicate_ids):
#         operations[f"delete_{idx}"] = f"crm.deal.delete?id={deal_id}"
#         logging.debug(f"Добавлена операция удаления: delete_{idx} для ID={deal_id}")
#
#     logging.info(f"Операции на удаление добавлены. Всего операций: {len(operations)}")


import requests
from config import webhook_url

# Выполнение запроса к методу bizproc.robot.list
response = requests.post(f'{webhook_url}bizproc.robot.list')

# Проверка успешности запроса
if response.status_code == 200:
    data = response.json()
    if 'result' in data:
        robots = data['result']
        print('Список зарегистрированных роботов:')
        for robot in robots:
            print(f'- {robot}')
    else:
        print('Ошибка: отсутствует ключ "result" в ответе.')
else:
    print(f'Ошибка: статус код {response.status_code}')