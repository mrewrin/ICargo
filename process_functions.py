import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from bot_instance import bot
from db_management import get_personal_code_by_chat_id, get_track_data_by_track_number, get_client_by_chat_id, \
    get_client_by_contact_id, delete_deal_by_track_number, get_chat_id_by_contact_id, save_final_deal_to_db, \
    update_final_deal_in_db, get_final_deal_from_db, get_name_track_by_track_number, find_deal_by_track, \
    update_tracked_deal, get_task_id_by_deal_id, delete_task_from_db, get_original_date_by_track, save_deal_history, \
    update_name_track_by_track_number
from bitrix_integration import update_contact_fields_in_bitrix


# Определение маппинга стадий для каждой воронки
stage_mapping = {
    'ПВ Астана №1': {
        'arrived': 'C6:NEW',
        'awaiting_pickup': 'C6:UC_VEHS4L',
        'archive': 'C6:LOSE',
        'issued': 'C6:WON'
    },
    'ПВ Астана №2': {
        'arrived': 'C2:NEW',
        'awaiting_pickup': 'C2:UC_8EQX6X',
        'archive': 'C2:LOSE',
        'issued': 'C2:WON'
    },
    'ПВ Караганда №1': {
        'arrived': 'C4:NEW',
        'awaiting_pickup': 'C4:UC_VOLZYJ',
        'archive': 'C4:LOSE',
        'issued': 'C4:WON'
    }
}

pickup_points = {
    "pv_karaganda_1": "Караганда 1",
    "pv_karaganda_2": "Караганда 2",
    "pv_astana_1": "Астана ESIL",
    "pv_astana_2": "Астана ALMATINSKIY"
}


async def send_notification_if_required(deal_info, chat_id, track_number, pickup_point):
    """
    Отправляет уведомление при успешном обновлении пользовательских полей.
    """
    logging.info(f"Получена информация о сделке для уведомления: {deal_info}")

    stage_id = deal_info.get('STAGE_ID')

    # Определяем соответствующие пункты выдачи и стадии для уведомления
    locations = {
        'pv_astana_1': "г.Астана, ПВ Астана ESIL",
        'pv_astana_2': "г.Астана, ПВ Астана SARY-ARKA",
        'pv_karaganda_1': "г.Караганда, ПВ №1"
    }
    status_code_list = {
        "C4:NEW": "г.Караганда, ПВ №1",
        "C6:NEW": "г.Астана, ПВ Астана ESIL",
        "C2:NEW": "г.Астана, ПВ Астана SARY-ARKA"
    }
    location_value = locations.get(pickup_point, "неизвестное место выдачи")
    stage_value = status_code_list.get(stage_id)
    personal_code = get_personal_code_by_chat_id(chat_id)
    name_track = get_name_track_by_track_number(track_number)  # Получаем name_track из БД

    logging.info(f"Проверка уведомления: стадия сделки={stage_id}, пункт выдачи={location_value}, "
                 f"ожидаемая стадия={stage_value}, chat_id={chat_id}")

    # Проверяем условия отправки уведомления
    if location_value == stage_value and chat_id:
        try:
            message_text = f"Ваш заказ {name_track or ''} с трек номером {track_number} " \
                           f"прибыл в пункт выдачи {location_value}."
            await bot.send_message(chat_id=chat_id, text=message_text)
            logging.info(f"Уведомление отправлено пользователю с chat_id: {chat_id}")
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
    else:
        logging.info(f"Уведомление не отправлено: "
                     f"стадия {stage_id} или локация {location_value} не соответствуют требуемым условиям.")


# async def process_deal_add(deal_info, operations, unregistered_deals):
#     deal_id = deal_info.get('ID')
#     logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")
#
#     # Проверка, является ли сделка итоговой
#     if deal_info.get('UF_CRM_1729539412') == '1':
#         logging.info(f"Сделка с ID {deal_id} является итоговой и не будет обработана.")
#
#         # # Обновляем final_deal_id в базе данных
#         # creation_date = deal_info.get('DATE_CREATE').split('T')[0]  # Извлечение даты создания
#         # update_final_deal_id(contact_id, creation_date, deal_id)
#
#         return
#
#     # Этап и категория
#     stage_id = deal_info.get('STAGE_ID')
#     category_id = deal_info.get('CATEGORY_ID')
#     date_modify = deal_info.get('DATE_MODIFY')
#     awaiting_pickup_stages = {v['awaiting_pickup'] for v in stage_mapping.values()}
#
#     # Пропускаем обработку, если сделка уже на этапе 'awaiting_pickup'
#     if stage_id in awaiting_pickup_stages:
#         logging.info(f"Сделка с ID {deal_id} находится на этапе 'awaiting_pickup' и не будет обработана.")
#         return
#
#     contact_id = deal_info.get('CONTACT_ID')
#     track_number = deal_info.get('UF_CRM_1723542556619', '')
#     weight = deal_info.get('UF_CRM_1727870320443', 0)
#     amount = deal_info.get('OPPORTUNITY', 0)
#     number_of_orders = deal_info.get('UF_CRM_1730185262', 0)
#
#     # Получаем текущую запись из deal_history
#     deal_history = get_original_date_by_track(track_number)
#     if deal_history:
#         last_modified, saved_stage_id, china_shipment_date = deal_history
#
#         # Если этап изменился, обновляем `deal_history`
#         if saved_stage_id != stage_id:
#             logging.info(f"Этап сделки изменился: {saved_stage_id} -> {stage_id}. Обновляем запись в deal_history.")
#
#             # Определяем значение для china_shipment_date (заполняем только при нужном этапе)
#             china_date = date_modify if stage_id == "C8:PREPARATION" and china_shipment_date is None else china_shipment_date
#
#             save_deal_history(
#                 deal_id=deal_id,
#                 track_number=track_number,
#                 original_date_modify=date_modify,  # Сохраняем новое значение DATE_MODIFY
#                 stage_id=stage_id,  # Сохраняем новый этап
#                 china_shipment_date=china_date  # Заполняем дату отгрузки только один раз
#             )
#         else:
#             logging.info(f"Этап сделки не изменился. Обновление deal_history не требуется.")
#     else:
#         # Если записи в deal_history нет, создаём новую
#         logging.info(f"Запись в deal_history отсутствует. Создаём новую запись.")
#
#         china_date = date_modify if stage_id == "C8:PREPARATION" else None
#
#         save_deal_history(
#             deal_id=deal_id,
#             track_number=track_number,
#             original_date_modify=date_modify,
#             stage_id=stage_id,
#             china_shipment_date=china_date
#         )
#
#     # Логика для альтернативной категории 8
#     if int(category_id) == 8 and track_number:
#         logging.info(f"Начата обработка для категории 8. ID сделки: {deal_id}, трек-номер: {track_number}")
#
#         track_data = get_track_data_by_track_number(track_number)
#         if track_data:
#             chat_id = track_data.get('chat_id')
#             logging.info(f"Найдены данные по трек-номеру {track_number}: {track_data}")
#
#             client_info = get_client_by_chat_id(chat_id)
#             if client_info:
#                 logging.info(f"Получены данные клиента по chat_id {chat_id}: {client_info}")
#
#                 expected_contact_id = client_info['contact_id']
#                 old_deal_id = find_deal_by_track(track_number, current_deal_id=deal_id)
#                 logging.info(f"Ожидаемый контакт ID: {expected_contact_id}. Найдена старая сделка: {old_deal_id}")
#
#                 # Удаление старой сделки
#                 if old_deal_id and old_deal_id['ID'] != deal_id:
#                     logging.info(f"Удаление старой сделки ID {old_deal_id['ID']} для трек-номера {track_number}.")
#                     operations[
#                         f"detach_old_contact_{old_deal_id['ID']}"] = f"crm.deal.contact.items.delete?ID={old_deal_id['ID']}&CONTACT_ID={expected_contact_id}"
#                     operations[f"delete_old_deal_{old_deal_id['ID']}"] = f"crm.deal.delete?id={old_deal_id['ID']}"
#
#                 # Обновление текущей сделки
#                 title = f"{client_info['personal_code']} {client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
#                 pickup_mapping = {
#                     "pv_karaganda_1": "52",
#                     "pv_karaganda_2": "54",
#                     "pv_astana_1": "48",
#                     "pv_astana_2": "50"
#                 }
#                 pickup_point_mapped = pickup_mapping.get(client_info['pickup_point'])
#                 logging.info(f"Обновление сделки ID {deal_id}: новый заголовок: {title}")
#                 operations[f"update_deal_{deal_id}"] = (
#                     f"crm.deal.update?ID={deal_id}&fields[CONTACT_ID]={expected_contact_id}&fields[TITLE]={title}"
#                     f"&fields[PHONE]={client_info['phone']}&fields[CITY]={client_info['city']}"
#                     f"&fields[UF_CRM_1723542556619]={track_number}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
#                     f"&fields[UF_CRM_1725179625]={chat_id}"
#                 )
#                 # Обновляем таблицу tracked_deals
#                 update_tracked_deal(deal_id, track_number)
#
#                 logging.info(f"Операция обновления сделки добавлена для ID {deal_id}.")
#                 await send_notification_if_required(deal_info, chat_id, track_number, client_info['pickup_point'])
#
#                 # Проверяем этап "Прибыл в Алмату"
#                 almaty_stage_id = "C8:PREPAYMENT_INVOICE"
#
#                 if stage_id == almaty_stage_id:
#                     logging.info(
#                         f"Сделка {deal_id} находится на этапе 'Прибыл в Алмату'. Добавляем задачу с дедлайном через 3 дня.")
#
#                     # Заголовок и описание задачи
#                     task_title = f"Контроль этапа: сделка {deal_id}"
#                     task_description = (
#                         f"Сделка {deal_id} находится на этапе 'Прибыл в Алмату' более 3 дней. "
#                         f"Контакт: {client_info['phone']}, пункт выдачи: {client_info['pickup_point']}."
#                     )
#
#                     # Устанавливаем дату старта через 3 дня и дедлайн
#                     start_date = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S')
#                     deadline = (datetime.now() + timedelta(days=6)).strftime(
#                         '%Y-%m-%dT%H:%M:%S')  # Дедлайн через 6 дней
#
#                     # Добавляем задачу
#                     operations[f"almaty_task_{deal_id}"] = (
#                         f"tasks.task.add?"
#                         f"fields[TITLE]={task_title}&"
#                         f"fields[DESCRIPTION]={task_description}&"
#                         f"fields[RESPONSIBLE_ID]=1&"
#                         f"fields[PRIORITY]=2&"
#                         f"fields[UF_CRM_TASK]=D_{deal_id}&"
#                         f"fields[CREATED_DATE]={start_date}&"
#                         f"fields[DEADLINE]={deadline}"
#                     )
#                     logging.info(f"Задача для сделки {deal_id} добавлена в operations.")
#             else:
#                 logging.warning(f"Клиент с chat_id {chat_id} не найден. Проверка завершена.")
#         else:
#             logging.info(f"Трек-номер {track_number} не зарегистрирован в базе бота. Добавляем в список для обработки.")
#             unregistered_deals.append({
#                 'ID': deal_id,
#                 'track_number': track_number,
#                 'STAGE_ID': stage_id  # Добавляем этап сделки
#             })
#             # Проверяем этап "Прибыл в Алмату"
#             almaty_stage_id = "C8:PREPAYMENT_INVOICE"
#
#             if stage_id == almaty_stage_id:
#                 logging.info(
#                     f"Сделка {deal_id} находится на этапе 'Прибыл в Алмату'. Добавляем задачу с дедлайном через 3 дня.")
#
#                 # Заголовок и описание задачи
#                 task_title = f"Контроль этапа: сделка {deal_id}"
#                 task_description = (
#                     f"Сделка {deal_id} находится на этапе 'Прибыл в Алмату' более 3 дней. "
#                 )
#
#                 # Устанавливаем дату старта через 3 дня и дедлайн
#                 start_date = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S')
#                 deadline = (datetime.now() + timedelta(days=6)).strftime(
#                     '%Y-%m-%dT%H:%M:%S')  # Дедлайн через 6 дней
#
#                 # Добавляем задачу
#                 operations[f"almaty_task_{deal_id}"] = (
#                     f"tasks.task.add?"
#                     f"fields[TITLE]={task_title}&"
#                     f"fields[DESCRIPTION]={task_description}&"
#                     f"fields[RESPONSIBLE_ID]=1&"
#                     f"fields[PRIORITY]=2&"
#                     f"fields[UF_CRM_TASK]=D_{deal_id}&"
#                     f"fields[CREATED_DATE]={start_date}&"
#                     f"fields[DEADLINE]={deadline}"
#                 )
#                 logging.info(f"Задача для сделки {deal_id} добавлена в operations.")
#
#     else:
#         # Обработка для других категорий
#         pipeline_stage = {
#             6: 'ПВ Астана №1',
#             2: 'ПВ Астана №2',
#             4: 'ПВ Караганда №1'
#         }.get(int(category_id))
#         logging.info(
#             f"Начата обработка для других категорий. ID сделки: {deal_id}, трек-номер: {track_number}, категория: {category_id}"
#         )
#
#         client_info = None
#         if not track_number:
#             logging.warning("Трек-номер отсутствует. Пропуск обработки дубликатов.")
#             return
#
#         logging.info(f"Ищем данные по трек-номеру: {track_number}")
#         track_data = get_track_data_by_track_number(track_number)
#         if track_data:
#             chat_id = track_data.get('chat_id')
#             logging.info(f"Найдены данные по трек-номеру {track_number}: {track_data}")
#             client_info = get_client_by_chat_id(chat_id)
#             if client_info:
#                 logging.info(f"Получены данные клиента по chat_id {chat_id}: {client_info}")
#                 expected_contact_id = int(client_info.get('contact_id'))
#                 duplicate_deal = find_deal_by_track(track_number)
#                 if not duplicate_deal:
#                     logging.info(f"Дубликаты для трек-номера {track_number} не найдены.")
#                 else:
#                     logging.info(f"Ожидаемый контакт ID: {expected_contact_id}. Найден дубликат: {duplicate_deal}")
#
#                     # Обработка найденного дубликата
#                     if duplicate_deal['ID'] == deal_id:
#                         logging.info(f"Текущая сделка ID {deal_id} пропущена из обработки дубликатов.")
#                     else:
#                         old_stage_id = duplicate_deal.get('STAGE_ID')
#                         expected_stage_id = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
#                         # Удаление дубликата
#                         if not duplicate_deal.get('CONTACT_ID') or old_stage_id != expected_stage_id:
#                             logging.info(
#                                 f"Удаление дубликата ID {duplicate_deal['ID']} для трек-номера {track_number}. Этап дубликата: {old_stage_id}, ожидаемый этап: {expected_stage_id}."
#                             )
#
#                             # Добавляем операцию удаления сделки
#                             operations[
#                                 f"delete_old_deal_{duplicate_deal['ID']}"] = f"crm.deal.delete?id={duplicate_deal['ID']}"
#
#                             # Получение TASK_ID по ID сделки
#                             task_id = get_task_id_by_deal_id(duplicate_deal['ID'])
#                             if task_id:
#                                 # Добавляем операцию удаления задачи
#                                 operations[f"delete_task_{task_id}"] = f"tasks.task.delete?taskId={task_id}"
#                                 logging.info(
#                                     f"Добавлена операция удаления задачи с ID {task_id} для дубликата {duplicate_deal['ID']}.")
#
#                                 # Удаление записи о задаче из базы данных
#                                 delete_task_from_db(duplicate_deal['ID'])
#                                 logging.info(
#                                     f"Удалена запись о задаче с task_id {task_id} для сделки {duplicate_deal['ID']} из базы данных.")
#                             else:
#                                 logging.info(f"Для дубликата {duplicate_deal['ID']} не найдена привязанная задача.")
#
#                 # Проверка и перепривязка контакта
#                 if contact_id != expected_contact_id:
#                     logging.info(
#                         f"Контакт ID {contact_id} отличается от ожидаемого {expected_contact_id}. Создание операции по отвязке."
#                     )
#                     operations[
#                         f"detach_contact_{deal_id}"
#                     ] = f"crm.deal.contact.items.delete?ID={deal_id}&CONTACT_ID={contact_id}"
#                     contact_id = expected_contact_id
#                     logging.info(f"Контакт успешно перепривязан к ID {contact_id}.")
#                 title = f"{client_info['personal_code']} {client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
#                 logging.info(f"Обновление сделки ID {deal_id}: новый заголовок: {title}")
#                 operations[f"update_deal_{deal_id}"] = (
#                     f"crm.deal.update?ID={deal_id}&fields[CONTACT_ID]={contact_id}&fields[TITLE]={title}"
#                     f"&fields[PHONE]={client_info['phone']}&fields[CITY]={client_info['city']}"
#                     f"&fields[UF_CRM_1723542556619]={track_number}&fields[UF_CRM_1723542922949]={client_info['pickup_point']}"
#                     f"&fields[UF_CRM_1725179625]={chat_id}"
#                 )
#                 logging.info(
#                     f"Отправка уведомления: deal_id={deal_id}, track_number={track_number}, pickup_point={client_info['pickup_point']}, deal_info={deal_info}"
#                 )
#                 await send_notification_if_required(deal_info, chat_id, track_number, client_info['pickup_point'])
#             else:
#                 logging.warning(f"Клиент с chat_id {chat_id} не найден.")
#         else:
#             logging.info(
#                 f"Трек-номер {track_number} не зарегистрирован в базе бота. Добавляем в список для обработки.")
#             unregistered_deals.append({
#                 'ID': deal_id,
#                 'track_number': track_number,
#                 'STAGE_ID': stage_id  # Добавляем этап сделки
#             })
#
#             # Получение chat_id и данных клиента
#             chat_id = get_chat_id_by_contact_id(contact_id)
#             if not chat_id:
#                 logging.error(f"chat_id не найден для contact_id {contact_id}. Уведомление невозможно отправить.")
#                 return
#
#             client_info = get_client_by_chat_id(chat_id)
#             if not client_info:
#                 logging.error(
#                     f"Клиентская информация не найдена для chat_id {chat_id}. Уведомление невозможно отправить.")
#                 return
#
#             try:
#                 await send_notification_if_required(deal_info, chat_id, track_number, client_info['pickup_point'])
#             except Exception as e:
#                 logging.error(f"Ошибка при отправке уведомления для track_number {track_number}: {e}")
#
#         if not client_info and contact_id:
#             logging.info(f"Попытка получения данных клиента по contact_id {contact_id}")
#             client_info = get_client_by_contact_id(contact_id)
#
#         if not client_info:
#             logging.error(f"Клиентская информация не найдена для сделки {deal_id}. Пропуск обработки.")
#             return
#
#         logging.info(f"Получена информация о клиенте для создания/обновления итоговой сделки: {client_info}")
#
#         today_date = datetime.now(timezone.utc).date()
#         final_deal = get_final_deal_from_db(contact_id)
#         logging.info(f"Проверяем наличие итоговой сделки для контакта ID {contact_id}. Найдено: {final_deal}")
#
#         expected_awaiting_pickup_stage = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
#         expected_issued_stage = stage_mapping.get(pipeline_stage, {}).get('issued')
#
#         logging.info(f"Получена итоговая сделка из базы: {final_deal}")
#         if final_deal:
#             logging.info(f"Итоговая сделка найдена. Анализируем этапы и обновления.")
#             final_deal_creation_date = datetime.strptime(final_deal['creation_date'], '%Y-%m-%d').date()
#             current_stage_id = final_deal['current_stage_id']
#
#             logging.info(
#                 f"Текущий этап итоговой сделки: {current_stage_id}, ожидаемый этап 'issued': {expected_issued_stage}, "
#                 f"ожидаемый этап 'awaiting_pickup': {expected_awaiting_pickup_stage}, дата создания: {final_deal_creation_date}, "
#                 f"сегодняшняя дата: {today_date}."
#             )
#
#             if current_stage_id == expected_issued_stage:
#                 logging.info(
#                     f"Итоговая сделка для контакта {contact_id} находится на этапе 'issued' и не требует обновления.")
#
#             elif final_deal_creation_date == today_date and current_stage_id == expected_awaiting_pickup_stage:
#                 logging.info(f"Итоговая сделка {final_deal['final_deal_id']} обновляется для контакта {contact_id}")
#                 current_track_numbers = final_deal['track_numbers']
#                 logging.info(f"Текущие трек-номера в итоговой сделке: {current_track_numbers}")
#                 updated_track_numbers = f"{current_track_numbers}, {track_number}".strip(
#                     ', ') if current_track_numbers else track_number
#                 logging.info(f"Обновленные трек-номера для итоговой сделки: {updated_track_numbers}")
#                 operations[f"update_track_numbers_{final_deal['final_deal_id']}"] = (
#                     f"crm.deal.update?id={final_deal['final_deal_id']}&fields[UF_CRM_1729115312]={updated_track_numbers}"
#                 )
#                 logging.info(f"Обновление трек-номеров в итоговой сделке: {updated_track_numbers}")
#                 # Маппинг для пункта выдачи
#                 pickup_mapping = {
#                     "pv_karaganda_1": "52",
#                     "pv_karaganda_2": "54",
#                     "pv_astana_1": "48",
#                     "pv_astana_2": "50"
#                 }
#                 pickup_point_mapped = pickup_mapping.get(client_info['pickup_point'], "неизвестно")
#
#                 archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
#                 operations[f"archive_deal_{deal_id}"] = (
#                     f"crm.deal.update?id={deal_id}"
#                     f"&fields[TITLE]={client_info['personal_code']} "
#                     f"{client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
#                     f"&fields[STAGE_ID]={archive_stage_id}"
#                     f"&fields[UF_CRM_1727870320443]=0"
#                     f"&fields[OPPORTUNITY]=0"
#                     f"&fields[UF_CRM_1725179625]={client_info['chat_id']}"
#                     f"&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
#                 )
#                 update_final_deal_in_db(final_deal['final_deal_id'], updated_track_numbers, current_stage_id)
#                 logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
#                 delete_result = await delete_deal_by_track_number(track_number)
#
#                 if delete_result:
#                     logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
#                 else:
#                     logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")
#
#             else:
#                 # Проверка необходимости создания новой итоговой сделки
#                 if final_deal_creation_date != today_date:
#                     # Маппинг для пункта выдачи
#                     pickup_mapping = {
#                         "pv_karaganda_1": "52",
#                         "pv_karaganda_2": "54",
#                         "pv_astana_1": "48",
#                         "pv_astana_2": "50"
#                     }
#                     pickup_point_mapped = pickup_mapping.get(client_info['pickup_point'], "неизвестно")
#                     # Обновляем текущую сделку как итоговую
#                     operations[f"update_deal_as_final_{deal_id}"] = (
#                         f"crm.deal.update?ID={deal_id}&fields[TITLE]=Итоговая сделка: {client_info['personal_code']} "
#                         f"{client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
#                         f"&fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={expected_awaiting_pickup_stage}"
#                         f"&fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
#                         f"&fields[UF_CRM_1727870320443]={weight}&fields[OPPORTUNITY]={amount}&fields[UF_CRM_1730185262]={number_of_orders}"
#                         f"&fields[UF_CRM_1729115312]={track_number}&fields[UF_CRM_1729539412]=1&fields[OPENED]=Y"
#                     )
#                     logging.info(f"Обновление текущей сделки как итоговой добавлено в операции: {deal_id}.")
#
#                     if f"update_contact_fields_{contact_id}" not in operations:
#                         operations[f"update_contact_fields_{contact_id}"] = (
#                             f"crm.contact.update?id={contact_id}&fields[UF_CRM_1726207792191]={weight}"
#                             f"&fields[UF_CRM_1726207809637]={amount}&fields[UF_CRM_1730182877]={number_of_orders}"
#                         )
#                         logging.info(f"Добавлена операция обновления данных контакта {contact_id}.")
#                     else:
#                         logging.info(f"Операция обновления данных контакта {contact_id} уже существует.")
#
#                     update_name_track_by_track_number(track_number, "Прибывшие посылки")
#                     # Создаём копию текущей сделки в архивном этапе
#                     archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
#                     operations[f"create_copy_of_deal_{contact_id}"] = (
#                         f"crm.deal.add?"
#                         f"fields[TITLE]={client_info['personal_code']} {client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}&"
#                         f"fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={archive_stage_id}&"
#                         f"fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_point_mapped}&"
#                         f"fields[UF_CRM_1727870320443]=0&fields[OPPORTUNITY]=0&"
#                         f"fields[UF_CRM_1725179625]={client_info['chat_id']}&fields[UF_CRM_1723542556619]={track_number}&"
#                         f"fields[UF_CRM_1729539412]=1"
#                     )
#                     logging.info(f"Создание копии сделки добавлено в операции: {deal_id}.")
#
#                     # Сохраняем текущую сделку как итоговую в базу данных
#                     save_final_deal_to_db(
#                         contact_id=contact_id,
#                         deal_id=deal_id,
#                         creation_date=today_date.isoformat(),
#                         track_number=track_number,
#                         current_stage_id=expected_awaiting_pickup_stage,  # Этап итоговой сделки
#                         weight=weight,
#                         amount=amount,
#                         number_of_orders=number_of_orders
#                     )
#                     logging.info(f"Обновлена и сохранена текущая сделка {deal_id} как итоговая в базу данных.")
#
#         if not final_deal:
#             logging.warning(
#                 f"Итоговая сделка не найдена для контакта ID {contact_id}. Текущая сделка будет обновлена как итоговая.")
#             logging.info(
#                 f"Обновление текущей сделки как итоговой с параметрами: контакт ID={contact_id}, трек-номер={track_number}, "
#                 f"вес={weight}, сумма={amount}, заказы={number_of_orders}, этап ожидания {expected_awaiting_pickup_stage}"
#             )
#
#             # Получение данных по клиенту
#             client_info = None
#
#             if track_number:
#                 track_data = get_track_data_by_track_number(track_number)
#                 if track_data:
#                     chat_id = track_data.get('chat_id')
#                     if chat_id:
#                         client_info = get_client_by_chat_id(chat_id)
#                         logging.info(f"Данные клиента успешно получены через chat_id: {client_info}")
#                         if not client_info:
#                             logging.warning(f"Клиент с chat_id {chat_id} не найден. Попробуем использовать contact_id.")
#                     else:
#                         logging.warning(f"Для трек-номера {track_number} отсутствует chat_id.")
#                 else:
#                     logging.warning(f"Данные по трек-номеру {track_number} не найдены.")
#
#             # Если данные через track_number не найдены, пробуем получить их через contact_id
#             if not client_info and contact_id:
#                 logging.info(f"Попытка получения данных клиента по contact_id {contact_id}.")
#                 client_info = get_client_by_contact_id(contact_id)
#                 if client_info:
#                     chat_id = client_info.get('chat_id')  # Убедитесь, что chat_id существует в client_info
#                     logging.info(f"Данные клиента успешно получены через contact_id: {client_info}")
#                 else:
#                     logging.warning(f"Клиент с contact_id {contact_id} не найден.")
#
#             # Проверка: если client_info все еще None, логируем ошибку
#             if not client_info:
#                 logging.error(
#                     f"Не удалось получить данные клиента для contact_id {contact_id} или track_number {track_number}.")
#                 return  # Или другое завершение обработки
#
#             # Маппинг для пункта выдачи
#             pickup_mapping = {
#                 "pv_karaganda_1": "52",
#                 "pv_karaganda_2": "54",
#                 "pv_astana_1": "48",
#                 "pv_astana_2": "50"
#             }
#             pickup_point_mapped = pickup_mapping.get(client_info['pickup_point'], "неизвестно")
#
#             # Обновляем текущую сделку как итоговую
#             operations[f"update_deal_as_final_{deal_id}"] = (
#                 f"crm.deal.update?ID={deal_id}&fields[TITLE]=Итоговая сделка: {client_info['personal_code']} "
#                 f"{client_info['name_translit']} {client_info['pickup_point']} {client_info['phone']}&fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={expected_awaiting_pickup_stage}"
#                 f"&fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
#                 f"&fields[UF_CRM_1727870320443]={weight}&fields[OPPORTUNITY]={amount}&fields[UF_CRM_1730185262]={number_of_orders}"
#                 f"&fields[UF_CRM_1729115312]={track_number}&fields[UF_CRM_1729539412]=1&fields[OPENED]=Y"
#             )
#             # Маппинг для проверки соответствия пункта выдачи стадии
#             task_mapping = {
#                 "52": "C4:NEW",
#                 "48": "C6:NEW",
#                 "50": "C2:NEW"
#             }
#             exp_stage = task_mapping.get(pickup_point_mapped)
#             # Проверяем соответствие пункта выдачи стадии
#             if stage_id != exp_stage:
#                 logging.warning(
#                     f"Несоответствие текущей стадии {stage_id} и ожидаемой стадии сделки {exp_stage}.")
#
#                 # Формируем новый заголовок для сделки
#                 incorrect_title = f"НЕВЕРНЫЙ ПУНКТ ВЫДАЧИ: {client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#
#                 # Добавляем операцию изменения поля TITLE в сделке
#                 operations[f"update_deal_title_{deal_id}"] = (
#                     f"crm.deal.update?ID={deal_id}&fields[TITLE]={incorrect_title}"
#                 )
#                 logging.info(f"Обновление TITLE для сделки {deal_id} на '{incorrect_title}' добавлено в operations.")
#
#                 # Формируем заголовок и описание задачи
#                 task_title = f"Некорректный пункт выдачи! Проверьте сделку {deal_id}"
#                 task_description = (
#                     f"Заказ прибыл в некорректный пункт выдачи: {client_info['pickup_point']}. "
#                     f"Ожидалась стадия: {exp_stage}, текущая стадия: {stage_id}. "
#                     f"Контакт: +{client_info['phone']}."
#                 )
#                 deadline = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
#
#                 # Добавляем операцию создания задачи
#                 operations[f"create_task_{deal_id}"] = (
#                     f"tasks.task.add?"
#                     f"fields[TITLE]={task_title}&"
#                     f"fields[DESCRIPTION]={task_description}&"
#                     f"fields[RESPONSIBLE_ID]=1&"
#                     f"fields[PRIORITY]=2&"
#                     f"fields[UF_CRM_TASK]=D_{deal_id}&"
#                     f"fields[DEADLINE]={deadline}"
#                 )
#                 logging.info(f"Операция создания задачи добавлена для сделки {deal_id}.")
#
#             logging.info(f"Обновлена текущая сделка {deal_id} как итоговая.")
#             title = f"{client_info['personal_code']} {client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
#             # Создание копии обрабатываемой сделки
#             archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
#             operations[f"create_final_deal_{contact_id}"] = (
#                 f"crm.deal.add?"
#                 f"fields[TITLE]={client_info['personal_code']} "
#                 f"{client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
#                 f"&fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={archive_stage_id}"
#                 f"&fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
#                 f"&fields[UF_CRM_1727870320443]=0&fields[OPPORTUNITY]=0"
#                 f"&fields[UF_CRM_1725179625]={chat_id}"
#                 f"&fields[UF_CRM_1723542556619]={track_number}&fields[UF_CRM_1729539412]=1"
#             )
#
#             logging.info(
#                 f"Создана операция для создания копии сделки. Трек-номер: {track_number}, контакт ID: {contact_id}")
#             logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
#             delete_result = await delete_deal_by_track_number(track_number)
#
#             if delete_result:
#                 logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
#             else:
#                 logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")
#
#             if f"update_contact_fields_{contact_id}" not in operations:
#                 operations[f"update_contact_fields_{contact_id}"] = (
#                     f"crm.contact.update?id={contact_id}&fields[UF_CRM_1726207792191]={weight}"
#                     f"&fields[UF_CRM_1726207809637]={amount}&fields[UF_CRM_1730182877]={number_of_orders}"
#                 )
#                 logging.info(f"Добавлена операция обновления данных контакта {contact_id}.")
#             else:
#                 logging.info(f"Операция обновления данных контакта {contact_id} уже существует.")
#
#             # Сохраняем текущую сделку как итоговую в базу данных
#             save_final_deal_to_db(
#                 contact_id=contact_id,
#                 deal_id=deal_id,
#                 creation_date=today_date.isoformat(),
#                 track_number=track_number,
#                 current_stage_id=expected_awaiting_pickup_stage,  # Передаем идентификатор из маппинга
#                 weight=weight,
#                 amount=amount,
#                 number_of_orders=number_of_orders
#             )
#             logging.info(f"Текущая сделка {deal_id} сохранена как итоговая в базу данных.")


class OperationsBuilder:
    """
    Класс для инкапсуляции формирования операций (batch-запросов) для обновления сделок в Bitrix24.
    Все методы добавляют соответствующие операции в словарь self.operations.
    """
    def __init__(self) -> None:
        self.operations: Dict[str, str] = {}

    def add_detach_old_contact(self, old_deal_id: int, expected_contact_id: str) -> None:
        """
        Добавляет операцию отвязки контакта от старой сделки.
        """
        key = f"detach_old_contact_{old_deal_id}"
        self.operations[key] = f"crm.deal.contact.items.delete?ID={old_deal_id}&CONTACT_ID={expected_contact_id}"

    def add_delete_deal(self, deal_id: int) -> None:
        """
        Добавляет операцию удаления сделки.
        """
        key = f"delete_deal_{deal_id}"
        self.operations[key] = f"crm.deal.delete?id={deal_id}"

    def add_update_deal(self, deal_id: int, expected_contact_id: str, title: str, phone: str, city: str,
                        track_number: str, pickup_point_mapped: str, chat_id: str) -> None:
        """
        Добавляет операцию обновления текущей сделки.
        """
        key = f"update_deal_{deal_id}"
        self.operations[key] = (
            f"crm.deal.update?ID={deal_id}&fields[CONTACT_ID]={expected_contact_id}&fields[TITLE]={title}"
            f"&fields[PHONE]={phone}&fields[CITY]={city}&fields[UF_CRM_1723542556619]={track_number}"
            f"&fields[UF_CRM_1723542922949]={pickup_point_mapped}&fields[UF_CRM_1725179625]={chat_id}"
        )

    def add_almaty_task(self, deal_id: int, client_phone: str, client_pickup: str) -> None:
        """
        Добавляет операцию создания задачи для сделки, находящейся на этапе 'Прибыл в Алмату'.
        """
        task_title = f"Контроль этапа: сделка {deal_id}"
        task_description = (
            f"Сделка {deal_id} находится на этапе 'Прибыл в Алмату' более 3 дней. "
            f"Контакт: {client_phone}, пункт выдачи: {client_pickup}."
        )
        start_date = (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S')
        deadline = (datetime.now() + timedelta(days=6)).strftime('%Y-%m-%dT%H:%M:%S')
        key = f"almaty_task_{deal_id}"
        self.operations[key] = (
            f"tasks.task.add?"
            f"fields[TITLE]={task_title}&"
            f"fields[DESCRIPTION]={task_description}&"
            f"fields[RESPONSIBLE_ID]=1&"
            f"fields[PRIORITY]=2&"
            f"fields[UF_CRM_TASK]=D_{deal_id}&"
            f"fields[CREATED_DATE]={start_date}&"
            f"fields[DEADLINE]={deadline}"
        )

    def add_update_contact_fields(self, contact_id: str, weight: Any, amount: Any, number_of_orders: Any) -> None:
        """
        Добавляет операцию обновления пользовательских полей контакта.
        """
        key = f"update_contact_fields_{contact_id}"
        self.operations[key] = (
            f"crm.contact.update?id={contact_id}&fields[UF_CRM_1726207792191]={weight}"
            f"&fields[UF_CRM_1726207809637]={amount}&fields[UF_CRM_1730182877]={number_of_orders}"
        )

    def add_update_deal_as_final(self, deal_id: int, client_info: Dict[str, Any], contact_id: str,
                                   expected_awaiting_pickup_stage: str, category_id: int,
                                   pickup_point_mapped: str, weight: Any, amount: Any,
                                   number_of_orders: Any, track_number: str) -> None:
        """
        Добавляет операцию обновления сделки как итоговой.
        """
        title = (f"Итоговая сделка: {client_info['personal_code']} {client_info['name_translit']} "
                 f"{client_info['pickup_point']} +{client_info['phone']}")
        key = f"update_deal_as_final_{deal_id}"
        self.operations[key] = (
            f"crm.deal.update?ID={deal_id}&fields[TITLE]={title}"
            f"&fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={expected_awaiting_pickup_stage}"
            f"&fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
            f"&fields[UF_CRM_1727870320443]={weight}&fields[OPPORTUNITY]={amount}"
            f"&fields[UF_CRM_1730185262]={number_of_orders}&fields[UF_CRM_1729115312]={track_number}"
            f"&fields[UF_CRM_1729539412]=1&fields[OPENED]=Y"
        )

    def add_create_copy_of_deal(self, contact_id: str, client_info: Dict[str, Any], stage_id: str,
                                category_id: int, pickup_point_mapped: str, chat_id: str,
                                track_number: str) -> None:
        """
        Добавляет операцию создания копии сделки в архивном этапе.
        """
        title = (f"{client_info['personal_code']} {client_info['name_translit']} "
                 f"{client_info['pickup_point']} +{client_info['phone']}")
        key = f"create_final_deal_{contact_id}"
        self.operations[key] = (
            f"crm.deal.add?"
            f"fields[TITLE]={title}&"
            f"fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={stage_id}"
            f"&fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
            f"&fields[UF_CRM_1727870320443]=0&fields[OPPORTUNITY]=0"
            f"&fields[UF_CRM_1725179625]={chat_id}&fields[UF_CRM_1723542556619]={track_number}"
            f"&fields[UF_CRM_1729539412]=1"
        )

    def add_update_deal_title(self, deal_id: int, incorrect_title: str) -> None:
        """
        Добавляет операцию обновления заголовка сделки.
        """
        key = f"update_deal_title_{deal_id}"
        self.operations[key] = f"crm.deal.update?ID={deal_id}&fields[TITLE]={incorrect_title}"

    def add_create_task(self, deal_id: int, task_title: str, task_description: str, deadline: str) -> None:
        """
        Добавляет операцию создания задачи для сделки.
        """
        key = f"create_task_{deal_id}"
        self.operations[key] = (
            f"tasks.task.add?"
            f"fields[TITLE]={task_title}&fields[DESCRIPTION]={task_description}"
            f"&fields[RESPONSIBLE_ID]=1&fields[PRIORITY]=2&fields[UF_CRM_TASK]=D_{deal_id}"
            f"&fields[DEADLINE]={deadline}"
        )

    def add_update_track_numbers(self, final_deal_id: int, updated_track_numbers: str) -> None:
        """
        Добавляет операцию обновления списка трек-номеров в итоговой сделке.
        """
        key = f"update_track_numbers_{final_deal_id}"
        self.operations[key] = f"crm.deal.update?id={final_deal_id}&fields[UF_CRM_1729115312]={updated_track_numbers}"


def precheck_deal(deal_info: dict) -> Optional[dict]:
    """
    Выполняет предварительные проверки и инициализацию параметров сделки.

    Проверки:
      - Если сделка помечена как итоговая (UF_CRM_1729539412 == '1'), функция возвращает None.
      - Если стадия сделки (STAGE_ID) входит в набор стадий "awaiting_pickup" (на основе stage_mapping),
      функция возвращает None.

    Если проверки пройдены, возвращает словарь с базовыми параметрами сделки:
      - deal_id
      - stage_id
      - category_id
      - date_modify
      - contact_id
      - track_number
      - weight
      - amount
      - number_of_orders
    """
    deal_id = deal_info.get('ID')
    logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")

    # Проверка, является ли сделка итоговой
    if deal_info.get('UF_CRM_1729539412') == '1':
        logging.info(f"Сделка с ID {deal_id} является итоговой и не будет обработана.")
        return None

    stage_id = deal_info.get('STAGE_ID')
    category_id = deal_info.get('CATEGORY_ID')
    date_modify = deal_info.get('DATE_MODIFY')

    # Формирование набора стадий, соответствующих "awaiting_pickup"
    awaiting_pickup_stages = {v['awaiting_pickup'] for v in stage_mapping.values()}
    if stage_id in awaiting_pickup_stages:
        logging.info(f"Сделка с ID {deal_id} находится на этапе 'awaiting_pickup' и не будет обработана.")
        return None

    contact_id = deal_info.get('CONTACT_ID')
    track_number = deal_info.get('UF_CRM_1723542556619', '')
    weight = deal_info.get('UF_CRM_1727870320443', 0)
    amount = deal_info.get('OPPORTUNITY', 0)
    number_of_orders = deal_info.get('UF_CRM_1730185262', 0)

    return {
        'deal_id': deal_id,
        'stage_id': stage_id,
        'category_id': category_id,
        'date_modify': date_modify,
        'contact_id': contact_id,
        'track_number': track_number,
        'weight': weight,
        'amount': amount,
        'number_of_orders': number_of_orders
    }


def update_deal_history(deal_id: int, track_number: str, stage_id: str, date_modify: str) -> None:
    """
    Обновляет или создает запись в deal_history для сделки.

    Если запись существует и этап изменился, обновляет запись (с учетом поля china_shipment_date).
    Если записи нет, создает новую.
    """
    deal_history = get_original_date_by_track(track_number)
    if deal_history:
        last_modified, saved_stage_id, china_shipment_date = deal_history
        if saved_stage_id != stage_id:
            logging.info(f"Этап сделки изменился: {saved_stage_id} -> {stage_id}. Обновляем запись в deal_history.")
            china_date = date_modify if stage_id == "C8:PREPARATION" and china_shipment_date is None \
                else china_shipment_date
            save_deal_history(
                deal_id=deal_id,
                track_number=track_number,
                original_date_modify=date_modify,
                stage_id=stage_id,
                china_shipment_date=china_date
            )
        else:
            logging.info("Этап сделки не изменился. Обновление deal_history не требуется.")
    else:
        logging.info("Запись в deal_history отсутствует. Создаём новую запись.")
        china_date = date_modify if stage_id == "C8:PREPARATION" else None
        save_deal_history(
            deal_id=deal_id,
            track_number=track_number,
            original_date_modify=date_modify,
            stage_id=stage_id,
            china_shipment_date=china_date
        )


async def process_order_pipeline(
    deal_info: Dict[str, Any],
    operations: Dict[str, str],
    unregistered_deals: List[Dict[str, Any]]
) -> None:
    """
    Обрабатывает сделки из воронки "Заказ" (категория 8) в Битрикс24.
    При наличии track_number пытается получить данные трека и клиента:
      - Если данные найдены, вызывается обработка существующего трека.
      - Если данные отсутствуют, сделка добавляется в список unregistered_deals,
        а при необходимости — создаётся операция на задачу.
    Итоговые операции собираются в объекте OperationsBuilder и затем объединяются с operations.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    stage_id: str = deal_info.get('STAGE_ID', '')

    logging.info(f"Начата обработка для категории 8. ID сделки: {deal_id}, трек-номер: {track_number}")
    if not track_number:
        return

    ops_builder = OperationsBuilder()

    try:
        track_data = get_track_data_by_track_number(track_number)
    except Exception as e:
        logging.error(f"Ошибка при получении данных трека {track_number}: {e}")
        return

    if track_data:
        await _process_existing_track(deal_info, track_data, ops_builder)
    else:
        await _process_unregistered_track(deal_info, stage_id, ops_builder, unregistered_deals)

    operations.update(ops_builder.operations)


async def _process_existing_track(
    deal_info: Dict[str, Any],
    track_data: Dict[str, Any],
    ops_builder: OperationsBuilder
) -> None:
    """
    Обрабатывает случай, когда данные по track_number найдены.
    Формирует операции обновления сделки, удаления дубликатов и, при необходимости, создания задачи.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    chat_id: Optional[str] = track_data.get('chat_id')
    logging.info(f"Найдены данные по трек-номеру {track_number}: {track_data}")

    try:
        client_info = get_client_by_chat_id(chat_id)
    except Exception as e:
        logging.error(f"Ошибка при получении данных клиента по chat_id {chat_id}: {e}")
        return

    if client_info:
        logging.info(f"Получены данные клиента по chat_id {chat_id}: {client_info}")
        expected_contact_id: str = client_info['contact_id']
        old_deal = find_deal_by_track(track_number, current_deal_id=deal_info.get('ID'))
        logging.info(f"Ожидаемый контакт ID: {expected_contact_id}. Найдена старая сделка: {old_deal}")

        if old_deal and old_deal['ID'] != deal_info.get('ID'):
            logging.info(f"Удаление старой сделки ID {old_deal['ID']} для трек-номера {track_number}.")
            ops_builder.add_detach_old_contact(old_deal['ID'], expected_contact_id)
            ops_builder.add_delete_deal(old_deal['ID'])

        title = f"{client_info['personal_code']} {client_info['name_translit']} " \
                f"{client_info['pickup_point']} +{client_info['phone']}"
        pickup_mapping: Dict[str, str] = {
            "pv_karaganda_1": "52",
            "pv_karaganda_2": "54",
            "pv_astana_1": "48",
            "pv_astana_2": "50"
        }
        pickup_point_mapped: Optional[str] = pickup_mapping.get(client_info['pickup_point'])
        logging.info(f"Обновление сделки ID {deal_info.get('ID')}: новый заголовок: {title}")
        ops_builder.add_update_deal(
            deal_id=deal_info.get('ID'),
            expected_contact_id=expected_contact_id,
            title=title,
            phone=client_info['phone'],
            city=client_info['city'],
            track_number=track_number,
            pickup_point_mapped=pickup_point_mapped,
            chat_id=chat_id
        )
        update_tracked_deal(deal_info.get('ID'), track_number)
        logging.info(f"Операция обновления сделки добавлена для ID {deal_info.get('ID')}.")

        try:
            await send_notification_if_required(deal_info, chat_id, track_number, client_info['pickup_point'])
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления: {e}")

        almaty_stage_id = "C8:PREPAYMENT_INVOICE"
        if deal_info.get('STAGE_ID') == almaty_stage_id:
            logging.info(f"Сделка {deal_info.get('ID')} находится на этапе 'Прибыл в Алмату'. Добавляем задачу с дедлайном через 3 дня.")
            ops_builder.add_almaty_task(deal_info.get('ID'), client_info['phone'], client_info['pickup_point'])
    else:
        logging.warning(f"Клиент с chat_id {chat_id} не найден. Проверка завершена.")


async def _process_unregistered_track(
    deal_info: Dict[str, Any],
    stage_id: str,
    ops_builder: OperationsBuilder,
    unregistered_deals: List[Dict[str, Any]]
) -> None:
    """
    Обрабатывает случай, когда данные по track_number не найдены.
    Добавляет сделку в список unregistered_deals и, если стадия соответствует 'Прибыл в Алмату',
    пытается получить данные клиента через contact_id и добавить операцию создания задачи.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    logging.info(f"Трек-номер {track_number} не зарегистрирован в базе бота. Добавляем в список для обработки.")
    unregistered_deals.append({
        'ID': deal_id,
        'track_number': track_number,
        'STAGE_ID': stage_id
    })
    almaty_stage_id = "C8:PREPAYMENT_INVOICE"
    if stage_id == almaty_stage_id:
        contact_id = deal_info.get('CONTACT_ID')
        chat_id = get_chat_id_by_contact_id(contact_id)
        if chat_id:
            client_info = get_client_by_chat_id(chat_id)
            if client_info:
                ops_builder.add_almaty_task(deal_id, client_info['phone'], client_info['pickup_point'])
                logging.info(f"Задача для сделки {deal_id} добавлена через OperationsBuilder.")
            else:
                logging.error(f"Клиентская информация не найдена для chat_id {chat_id}.")
        else:
            logging.error(f"chat_id не найден для contact_id {contact_id}. Уведомление невозможно отправить.")


async def process_pickup_pipelines(
        deal_info: Dict[str, Any],
        operations: Dict[str, str],
        unregistered_deals: List[Dict[str, Any]]
) -> None:
    """
    Обрабатывает сделки для воронок пунктов выдачи (не относящиеся к категории "Заказ").
    Определяет pipeline_stage на основе category_id, затем:
      - Если данные по track_number найдены, вызывается _process_existing_pickup.
      - Если данные отсутствуют, вызывается _process_unregistered_pickup.
    Итоговые операции собираются в OperationsBuilder и объединяются с operations.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    stage_id: str = deal_info.get('STAGE_ID', '')
    category_id = deal_info.get('CATEGORY_ID')

    pipeline_stage: Optional[str] = {
        6: 'ПВ Астана №1',
        2: 'ПВ Астана №2',
        4: 'ПВ Караганда №1'
    }.get(int(category_id))

    logging.info(
        f"Начата обработка для других категорий. ID сделки: {deal_id}, "
        f"трек-номер: {track_number}, категория: {category_id}")
    if not track_number:
        logging.warning("Трек-номер отсутствует. Пропуск обработки дубликатов.")
        return

    ops_builder = OperationsBuilder()

    try:
        track_data = get_track_data_by_track_number(track_number)
    except Exception as e:
        logging.error(f"Ошибка при получении данных трека {track_number}: {e}")
        return

    if track_data:
        await _process_existing_pickup(deal_info, track_data, pipeline_stage, ops_builder)
    else:
        await _process_unregistered_pickup(deal_info, stage_id, ops_builder, unregistered_deals)

    operations.update(ops_builder.operations)


async def _process_existing_pickup(
    deal_info: Dict[str, Any],
    track_data: Dict[str, Any],
    pipeline_stage: Optional[str],
    ops_builder: OperationsBuilder
) -> None:
    """
    Обрабатывает случай, когда данные по track_number найдены для воронок пунктов выдачи.
    Выполняет проверку на дубликаты, перепривязку контакта и обновление сделки с отправкой уведомления.
    Если данные клиента не получены через chat_id, происходит попытка получения через contact_id.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    chat_id: Optional[str] = track_data.get('chat_id')
    logging.info(f"Найдены данные по трек-номеру {track_number}: {track_data}")

    try:
        client_info = get_client_by_chat_id(chat_id)
    except Exception as e:
        logging.error(f"Ошибка при получении клиента по chat_id {chat_id}: {e}")
        client_info = None

    if not client_info:
        # Фоллбэк: если client_info не получен через chat_id, пробуем получить по contact_id
        contact_id = deal_info.get('CONTACT_ID')
        logging.info(f"Клиент не найден по chat_id {chat_id}. Попытка получения данных по contact_id {contact_id}.")
        client_info = get_client_by_contact_id(contact_id)
        if not client_info:
            logging.error(f"Клиент с contact_id {contact_id} не найден.")
            return

    logging.info(f"Получены данные клиента: {client_info}")
    expected_contact_id: int = int(client_info.get('contact_id'))
    duplicate_deal = find_deal_by_track(track_number, current_deal_id=deal_info.get('ID'))
    logging.info(f"Ожидаемый контакт ID: {expected_contact_id}. Найден дубликат: {duplicate_deal}")

    if duplicate_deal and duplicate_deal['ID'] != deal_info.get('ID'):
        old_stage_id = duplicate_deal.get('STAGE_ID')
        expected_stage_id = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
        if not duplicate_deal.get('CONTACT_ID') or old_stage_id != expected_stage_id:
            logging.info(
                f"Удаление дубликата ID {duplicate_deal['ID']} для трек-номера {track_number}. "
                f"Этап дубликата: {old_stage_id}, ожидаемый этап: {expected_stage_id}."
            )
            ops_builder.add_detach_old_contact(duplicate_deal['ID'], expected_contact_id)
            ops_builder.add_delete_deal(duplicate_deal['ID'])
            task_id = get_task_id_by_deal_id(duplicate_deal['ID'])
            if task_id:
                ops_builder.operations[f"delete_task_{task_id}"] = f"tasks.task.delete?taskId={task_id}"
                logging.info(f"Операция удаления задачи с ID {task_id} для дубликата {duplicate_deal['ID']} добавлена.")
                delete_task_from_db(duplicate_deal['ID'])
                logging.info(f"Запись о задаче для дубликата {duplicate_deal['ID']} удалена.")
            else:
                logging.info(f"Для дубликата {duplicate_deal['ID']} не найдена привязанная задача.")
    else:
        logging.info(f"Дубликаты для трек-номера {track_number} не найдены.")

    if chat_id != str(expected_contact_id):
        logging.info(
            f"Контакт ID {chat_id} отличается от ожидаемого {expected_contact_id}. Создание операции по отвязке.")
        ops_builder.operations[f"detach_contact_{deal_id}"] = f"crm.deal.contact.items.delete?ID={deal_id}&CONTACT_ID={chat_id}"
        chat_id = str(expected_contact_id)
        logging.info(f"Контакт успешно перепривязан к ID {chat_id}.")

    title = f"{client_info['personal_code']} {client_info['name_translit']} {client_info['pickup_point']} +{client_info['phone']}"
    logging.info(f"Обновление сделки ID {deal_id}: новый заголовок: {title}")
    ops_builder.add_update_deal(
        deal_id=deal_info.get('ID'),
        expected_contact_id=expected_contact_id,
        title=title,
        phone=client_info['phone'],
        city=client_info['city'],
        track_number=track_number,
        pickup_point_mapped={  # Локальный mapping
            "pv_karaganda_1": "52",
            "pv_karaganda_2": "54",
            "pv_astana_1": "48",
            "pv_astana_2": "50"
        }.get(client_info['pickup_point']),
        chat_id=chat_id
    )
    update_tracked_deal(deal_info.get('ID'), track_number)
    logging.info(f"Операция обновления сделки добавлена для ID {deal_info.get('ID')}.")
    try:
        await send_notification_if_required(deal_info, chat_id, track_number, client_info['pickup_point'])
    except Exception as e:
        logging.error(f"Ошибка при отправке уведомления: {e}")


async def _process_unregistered_pickup(
    deal_info: Dict[str, Any],
    stage_id: str,
    ops_builder: OperationsBuilder,
    unregistered_deals: List[Dict[str, Any]]
) -> None:
    """
    Обрабатывает случай, когда данные по track_number отсутствуют для воронок пунктов выдачи.
    Добавляет сделку в unregistered_deals и, если стадия соответствует 'Прибыл в Алмату',
    пытается получить данные клиента через chat_id, а при неудаче – через contact_id,
    для отправки уведомления.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    logging.info(f"Трек-номер {track_number} не зарегистрирован в базе бота. Добавляем в список для обработки.")
    unregistered_deals.append({
        'ID': deal_id,
        'track_number': track_number,
        'STAGE_ID': stage_id
    })
    almaty_stage_id = "C8:PREPAYMENT_INVOICE"
    if stage_id == almaty_stage_id:
        contact_id = deal_info.get('CONTACT_ID')
        chat_id = get_chat_id_by_contact_id(contact_id)
        if not chat_id:
            logging.error(f"chat_id не найден для contact_id {contact_id}. Уведомление невозможно отправить.")
            return
        try:
            client_info = get_client_by_chat_id(chat_id)
        except Exception as e:
            logging.error(f"Ошибка при получении клиента по chat_id {chat_id}: {e}")
            client_info = None
        if not client_info:
            logging.info(f"Данные по chat_id {chat_id} не получены. Попытка получения через contact_id {contact_id}.")
            client_info = get_client_by_contact_id(contact_id)
            if not client_info:
                logging.error(f"Клиент с contact_id {contact_id} не найден.")
                return
            logging.info(f"Данные клиента получены через contact_id: {client_info}")
        try:
            await send_notification_if_required(deal_info, chat_id, track_number, client_info['pickup_point'])
            logging.info(f"Уведомление для сделки {deal_id} отправлено через _process_unregistered_pickup.")
        except Exception as e:
            logging.error(f"Ошибка при отправке уведомления для track_number {track_number}: {e}")


async def process_final_deal(
        deal_info: Dict[str, Any],
        client_info: Dict[str, Any],
        pipeline_stage: str,
        ops_builder: OperationsBuilder
) -> None:
    """
    Обрабатывает итоговую (final) сделку.

    Функция проверяет наличие итоговой сделки для данного контакта и, в зависимости от ее состояния,
    либо обновляет существующую итоговую сделку, либо обновляет текущую сделку как итоговую.
    Операции для batch-запросов добавляются в объект ops_builder.
    """
    contact_id = deal_info.get('CONTACT_ID')
    today_date = datetime.now(timezone.utc).date()

    final_deal = get_final_deal_from_db(contact_id)
    logging.info(f"Проверяем наличие итоговой сделки для контакта {contact_id}. Найдено: {final_deal}")

    expected_awaiting_pickup_stage = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
    expected_issued_stage = stage_mapping.get(pipeline_stage, {}).get('issued')

    if final_deal:
        final_deal_creation_date = datetime.strptime(final_deal['creation_date'], '%Y-%m-%d').date()
        current_stage_id = final_deal['current_stage_id']
        logging.info(
            f"Текущий этап итоговой сделки: {current_stage_id}, ожидаемый 'issued': {expected_issued_stage}, "
            f"ожидаемый 'awaiting_pickup': {expected_awaiting_pickup_stage}, "
            f"дата создания: {final_deal_creation_date}, "
            f"сегодня: {today_date}."
        )

        if current_stage_id == expected_issued_stage:
            logging.info(
                f"Итоговая сделка для контакта {contact_id} находится на этапе 'issued' и не требует обновления.")
        elif final_deal_creation_date == today_date and current_stage_id == expected_awaiting_pickup_stage:
            await _update_existing_final_deal(deal_info, final_deal, client_info, pipeline_stage, ops_builder)
        else:
            await _update_current_deal_as_final(deal_info, client_info, pipeline_stage, ops_builder)
    else:
        logging.warning(
            f"Итоговая сделка не найдена для контакта {contact_id}. Текущая сделка будет обновлена как итоговая.")
        await _update_current_deal_as_final(deal_info, client_info, pipeline_stage, ops_builder)


async def _update_existing_final_deal(
        deal_info: Dict[str, Any],
        final_deal: Dict[str, Any],
        client_info: Dict[str, Any],
        pipeline_stage: str,
        ops_builder: OperationsBuilder
) -> None:
    """
    Обновляет существующую итоговую сделку:
      - Обновляет список трек-номеров в итоговой сделке.
      - Если значения веса, суммы и количества заказов отличаются – суммирует их с новыми и
        вызывает метод add_update_deal_as_final для обновления всех полей.
      - Если значения совпадают – обновляет только список трек‑номеров.
      - Затем обновляет запись в базе и удаляет сделку по track_number.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    current_track_numbers = final_deal.get('track_numbers', '')
    updated_track_numbers = (
        f"{current_track_numbers}, {track_number}".strip(', ')
        if current_track_numbers else track_number
    )

    # Обновляем только трек‑номера, если агрегированные данные не изменились
    final_weight = final_deal.get('total_weight', 0)
    final_amount = final_deal.get('total_amount', 0)
    final_orders = final_deal.get('number_of_orders', 0)

    weight = deal_info.get('UF_CRM_1727870320443', 0)
    amount = deal_info.get('OPPORTUNITY', 0)
    number_of_orders = deal_info.get('UF_CRM_1730185262', 0)

    pickup_mapping = {
        "pv_karaganda_1": "52",
        "pv_karaganda_2": "54",
        "pv_astana_1": "48",
        "pv_astana_2": "50"
    }
    pickup_point_mapped = pickup_mapping.get(client_info['pickup_point'], "неизвестно")
    archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')

    if (weight, amount, number_of_orders) != (final_weight, final_amount, final_orders):
        new_weight = final_weight + weight
        new_amount = final_amount + amount
        new_orders = final_orders + number_of_orders
        logging.info(
            f"Обновление итоговой сделки: суммирование данных - вес: {final_weight} + {weight} = {new_weight}, "
            f"сумма: {final_amount} + {amount} = {new_amount}, заказы: {final_orders} + {number_of_orders} = {new_orders}"
        )
        # Обновляем итоговую сделку, включая поля веса, суммы и количества заказов
        ops_builder.add_update_deal_as_final(
            deal_id=deal_id,
            client_info=client_info,
            contact_id=client_info['contact_id'],
            expected_awaiting_pickup_stage=stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup'),
            category_id=final_deal.get('category_id', 0),
            pickup_point_mapped=pickup_point_mapped,
            weight=new_weight,
            amount=new_amount,
            number_of_orders=new_orders,
            track_number=updated_track_numbers
        )
        ops_builder.add_update_contact_fields(client_info['contact_id'], new_weight, new_amount, new_orders)
        # Обновляем данные в базе с дополнительными значениями
        update_final_deal_in_db(final_deal['final_deal_id'], updated_track_numbers,
                                final_deal['current_stage_id'],
                                weight=new_weight, amount=new_amount, orders=new_orders)
    else:
        # Если значения совпадают, обновляем только список трек‑номеров
        ops_builder.add_update_track_numbers(final_deal['final_deal_id'], updated_track_numbers)
        update_final_deal_in_db(final_deal['final_deal_id'], updated_track_numbers, final_deal['current_stage_id'])

    logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
    delete_result = await delete_deal_by_track_number(track_number)
    if delete_result:
        logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
    else:
        logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")


async def _update_current_deal_as_final(
        deal_info: Dict[str, Any],
        client_info: Dict[str, Any],
        pipeline_stage: str,
        ops_builder: OperationsBuilder
) -> None:
    """
    Обновляет текущую сделку как итоговую, если итоговая сделка отсутствует
    или создана не сегодня.
    Добавляет операции:
      - Обновление сделки как итоговой.
      - Обновление данных контакта.
      - Создание копии сделки.
    Сохраняет текущую сделку как итоговую в базе данных.
    """
    deal_id: Optional[int] = deal_info.get('ID')
    track_number: str = deal_info.get('UF_CRM_1723542556619', '')
    contact_id = deal_info.get('CONTACT_ID')
    category_id = deal_info.get('CATEGORY_ID')
    weight = deal_info.get('UF_CRM_1727870320443', 0)
    amount = deal_info.get('OPPORTUNITY', 0)
    number_of_orders = deal_info.get('UF_CRM_1730185262', 0)
    today_date = datetime.now(timezone.utc).date()
    logging.info(f'{pipeline_stage}')
    pickup_mapping: Dict[str, str] = {
        "pv_karaganda_1": "52",
        "pv_karaganda_2": "54",
        "pv_astana_1": "48",
        "pv_astana_2": "50"
    }
    pickup_point_mapped: str = pickup_mapping.get(client_info['pickup_point'], "неизвестно")

    ops_builder.add_update_deal_as_final(
        deal_id=deal_id,
        client_info=client_info,
        contact_id=contact_id,
        expected_awaiting_pickup_stage=stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup'),
        category_id=category_id,
        pickup_point_mapped=pickup_point_mapped,
        weight=weight,
        amount=amount,
        number_of_orders=number_of_orders,
        track_number=track_number
    )
    if f"update_contact_fields_{contact_id}" not in ops_builder.operations:
        ops_builder.add_update_contact_fields(contact_id, weight, amount, number_of_orders)
        logging.info(f"Операция обновления данных контакта {contact_id} добавлена.")
    else:
        logging.info(f"Операция обновления данных контакта {contact_id} уже существует.")
    update_name_track_by_track_number(track_number, "Прибывшие посылки")
    archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
    ops_builder.add_create_copy_of_deal(contact_id, client_info, archive_stage_id, category_id, pickup_point_mapped,
                                        client_info['chat_id'], track_number)
    logging.info(f"Создание копии сделки добавлено в операции: {deal_id}.")
    logging.info(f"{stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')}")
    save_final_deal_to_db(
        contact_id=contact_id,
        deal_id=deal_id,
        creation_date=today_date.isoformat(),
        track_number=track_number,
        current_stage_id=stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup'),
        weight=weight,
        amount=amount,
        number_of_orders=number_of_orders
    )
    logging.info(f"Обновлена и сохранена текущая сделка {deal_id} как итоговая в базу данных.")


async def process_deal_add(
        deal_info: Dict[str, Any],
        operations: Dict[str, str],
        unregistered_deals: List[Dict[str, Any]]
) -> None:
    """
    Итоговая интеграция обработки сделки.

    Последовательно выполняет:
      1. Предварительную проверку сделки.
      2. Обновление истории сделки.
      3. Обработку сделки по категориям:
         - process_order_pipeline для категории "Заказ" (8)
         - process_pickup_pipelines для остальных
      4. Обработку итоговой сделки.

    Все сформированные операции записываются в operations, который затем используется
    в batch_send_to_bitrix для формирования batch‑запросов.
    """
    # 1. Предварительная проверка
    precheck = precheck_deal(deal_info)
    if precheck is None:
        return

    # 2. Обновление истории сделки
    update_deal_history(
        precheck['deal_id'],
        precheck['track_number'],
        precheck['stage_id'],
        precheck['date_modify']
    )

    # 3. Обработка сделки по категориям
    category_id = precheck['category_id']
    if int(category_id) == 8:
        await process_order_pipeline(deal_info, operations, unregistered_deals)
    else:
        await process_pickup_pipelines(deal_info, operations, unregistered_deals)

    # 4. Обработка итоговой сделки
    pipeline_stage = {
        6: 'ПВ Астана №1',
        2: 'ПВ Астана №2',
        4: 'ПВ Караганда №1'
    }.get(int(category_id))
    client_info = None
    chat_id = get_chat_id_by_contact_id(precheck['contact_id'])
    if chat_id:
        client_info = get_client_by_chat_id(chat_id)
    if not client_info and precheck['contact_id']:
        logging.info(f"Попытка получения данных клиента по contact_id {precheck['contact_id']}")
        client_info = get_client_by_contact_id(precheck['contact_id'])
    if not client_info:
        logging.error(f"Клиентская информация не найдена для contact_id {precheck['contact_id']}. Пропуск обработки.")
        return
    final_ops_builder = OperationsBuilder()
    await process_final_deal(deal_info, client_info, pipeline_stage, final_ops_builder)
    operations.update(final_ops_builder.operations)

    # Результатом работы функции является обновленный словарь operations,
    # который позже используется для отправки batch‑запросов.


async def process_contact_update(contact_info):
    contact_id = contact_info.get('ID')
    logging.info(f"Обработка события ONCRMCONTACTUPDATE для контакта с ID: {contact_id}")

    if not contact_id:
        logging.warning("Не указан контактный ID.")
        return

    # Получаем chat_id по contact_id
    chat_id = get_chat_id_by_contact_id(contact_id)
    if not chat_id:
        logging.warning(f"chat_id для контакта {contact_id} не найден.")
        return

    # Получаем данные из локальной базы данных
    client_data = get_client_by_chat_id(chat_id)
    if not client_data:
        logging.warning(f"Данные клиента для chat_id {chat_id} не найдены.")
        return

    # Извлекаем данные для сравнения
    name_translit_db = client_data['name_translit']
    phone_db = client_data['phone']
    personal_code = client_data['personal_code']

    # Данные контакта из CRM
    name_translit_crm = contact_info.get('UF_CRM_1730093824027')
    phone_crm = contact_info.get('PHONE', [{}])[0].get('VALUE', '')

    # Логируем данные для отладки
    logging.info(f"Данные из CRM - Имя: {name_translit_crm}, Телефон: {phone_crm}")
    logging.info(f"Данные из базы - Имя: {name_translit_db}, Телефон: {phone_db}")

    # Проверка на изменения
    if (name_translit_crm != name_translit_db) or (phone_crm != phone_db):
        logging.info(f"Обнаружены изменения в контактной информации для контакта {contact_id}. "
                     f"Уведомление не отправлено.")
    else:
        # Извлекаем значения пользовательских полей для уведомления
        weight = contact_info.get('UF_CRM_1726207792191')
        amount = contact_info.get('UF_CRM_1726207809637')
        number_of_orders = contact_info.get('UF_CRM_1730182877')
        locations = {
            'pv_astana_1': "г.Астана, ПВ Астана ESIL",
            'pv_astana_2': "г.Астана, ПВ Астана SARY-ARKA",
            'pv_karaganda_1': "г.Караганда, ПВ №1"
        }
        location = locations.get(client_data['pickup_point'])
        # Отправляем уведомление только если поле amount заполнено и не равно нулю
        if amount and amount != '0':
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Посылки поступили в пункт выдачи {location} \n"
                         f"⚖ Вес заказов: {weight} кг.\n"
                         f"💰 Сумма оплаты по весу: {amount} тг.\n"
                         f"📦 Количество заказов к выдаче: {number_of_orders}.\n"
                         f"Ваш личный код: {personal_code}."
                )
                logging.info(f"Уведомление с весом и суммой отправлено пользователю с chat_id: {chat_id}")
                # Очищаем пользовательские поля контакта, устанавливая значения в 0
                await update_contact_fields_in_bitrix(contact_id, sum_weight=0, sum_amount=0, order_count=0)
                logging.info(f"Пользовательские поля контакта с ID {contact_id} успешно очищены.")

            except Exception as e:
                logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
        else:
            logging.info("Поле 'Сумма заказов' не заполнено или равно нулю. Уведомление не отправлено.")


async def process_deal_update(deal_info):
    """
    Обработка события ONCRMDEALUPDATE.
    Если STAGE_ID изменился на один из ['C2:WON', 'C6:WON', 'C4:WON'],
    удаляем сделку по трек-номеру из базы данных бота.
    """
    deal_id = deal_info.get('ID')
    try:
        # Извлекаем необходимые данные из информации о сделке
        stage_id = deal_info.get('STAGE_ID')
        track_number = deal_info.get('UF_CRM_1723542556619')  # Поле с трек-номером

        # Логируем данные для отладки
        logging.info(f"Обработка обновления сделки ID: {deal_id}, STAGE_ID: {stage_id}, трек-номер: {track_number}")

        # Проверяем, изменился ли STAGE_ID на один из указанных
        target_stages = ['C2:WON', 'C6:WON', 'C4:WON']
        if stage_id in target_stages:
            if track_number:
                # Удаляем сделку только из базы данных
                await delete_deal_by_track_number(track_number)
                logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
            else:
                logging.warning(f"Трек-номер для сделки ID {deal_id} отсутствует.")
        else:
            logging.info(
                f"STAGE_ID сделки ID {deal_id} не входит в список {target_stages}. Никаких действий не требуется.")
    except Exception as e:
        logging.error(f"Ошибка при обработке обновления сделки ID {deal_id}: {e}")
