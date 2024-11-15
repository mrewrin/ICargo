import logging
from datetime import datetime, timezone
from bot_instance import bot
from config import bitrix
from bitrix_integration import get_deal_info, find_deal_by_track_number
from db_management import get_personal_code_by_chat_id, get_track_data_by_track_number, get_client_by_chat_id, \
    get_client_by_contact_id, delete_deal_by_track_number, get_chat_id_by_contact_id, save_final_deal_to_db, \
    update_final_deal_in_db, get_final_deal_from_db


# Определение маппинга стадий для каждой воронки
stage_mapping = {
    'ПВ Астана №1': {
        'arrived': 'NEW',
        'awaiting_pickup': 'UC_MJZYDP',
        'archive': 'LOSE',
        'issued': 'WON'
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
    },
    'ПВ Караганда №2': {
        'arrived': 'C6:NEW',
        'awaiting_pickup': 'C6:UC_VEHS4L',
        'archive': 'C6:LOSE',
        'issued': 'C6:WON'
    }
}


async def send_notification_if_required(deal_id, track_number, pickup_point):
    """
    Отправляет уведомление при успешном обновлении пользовательских полей.
    """
    # Получаем информацию о сделке
    deal_info = await get_deal_info(deal_id)
    if not deal_info:
        logging.error(f"Не удалось получить информацию о сделке {deal_id} для отправки уведомления.")
        return

    stage_id = deal_info.get('STAGE_ID')
    chat_id = deal_info.get('UF_CRM_1725179625')

    # Определяем соответствующие пункты выдачи и стадии для уведомления
    locations = {
        '48': "г.Астана, ПВ №1",
        '50': "г.Астана, ПВ №2",
        '52': "г.Караганда, ПВ №1",
        '54': "г.Караганда, ПВ №2"
    }
    status_code_list = {
        "C4:NEW": "г.Караганда, ПВ №1",
        "C6:NEW": "г.Караганда, ПВ №2",
        "NEW": "г.Астана, ПВ №1",
        "C2:NEW": "г.Астана, ПВ №2"
    }
    location_value = locations.get(deal_info.get('UF_CRM_1723542922949'), "неизвестное место выдачи")
    stage_value = status_code_list.get(stage_id)
    personal_code = get_personal_code_by_chat_id(chat_id)
    logging.info(f"Проверка уведомления: стадия сделки={stage_id}, пункт выдачи={location_value}, "
                 f"ожидаемая стадия={stage_value}, chat_id={chat_id}")

    # Проверяем условия отправки уведомления
    if location_value == stage_value and chat_id:
        try:
            message_text = f"Ваш заказ с трек номером {track_number} прибыл в пункт выдачи {location_value}."
            if personal_code:
                message_text += f"\nВаш личный код: 讠AUG{personal_code}."
            await bot.send_message(chat_id=chat_id, text=message_text)
            logging.info(f"Уведомление отправлено пользователю с chat_id: {chat_id}")
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
    else:
        logging.info(f"Уведомление не отправлено: "
                     f"стадия {stage_id} или локация {location_value} не соответствуют требуемым условиям.")


async def process_deal_add(deal_info, operations):
    deal_id = deal_info.get('ID')
    logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")

    # Проверка, является ли сделка итоговой
    if deal_info.get('UF_CRM_1729539412') == '1':
        logging.info(f"Сделка с ID {deal_id} является итоговой и не будет обработана.")
        return

    # Этап и категория
    stage_id = deal_info.get('STAGE_ID')
    category_id = deal_info.get('CATEGORY_ID')
    awaiting_pickup_stages = {v['awaiting_pickup'] for v in stage_mapping.values()}

    # Пропускаем обработку, если сделка уже на этапе 'awaiting_pickup'
    if stage_id in awaiting_pickup_stages:
        logging.info(f"Сделка с ID {deal_id} находится на этапе 'awaiting_pickup' и не будет обработана.")
        return

    contact_id = deal_info.get('CONTACT_ID')
    track_number = deal_info.get('UF_CRM_1723542556619', '')
    weight = deal_info.get('UF_CRM_1727870320443', 0)
    amount = deal_info.get('OPPORTUNITY', 0)
    number_of_orders = deal_info.get('UF_CRM_1730185262', 0)

    # Логика для альтернативной категории 8
    if int(category_id) == 8 and track_number:
        logging.info(f"Начата обработка для категории 8. ID сделки: {deal_id}, трек-номер: {track_number}")

        track_data = get_track_data_by_track_number(track_number)
        if track_data:
            chat_id = track_data.get('chat_id')
            logging.info(f"Найдены данные по трек-номеру {track_number}: {track_data}")

            client_info = get_client_by_chat_id(chat_id)
            if client_info:
                logging.info(f"Получены данные клиента по chat_id {chat_id}: {client_info}")

                expected_contact_id = client_info['contact_id']
                old_deal_id = find_deal_by_track_number(track_number)
                logging.info(f"Ожидаемый контакт ID: {expected_contact_id}. Найдено старое deal_id: {old_deal_id}")

                # Проверка и перепривязка контакта
                if contact_id != expected_contact_id:
                    if contact_id:
                        logging.info(
                            f"Контакт ID {contact_id} отличается от ожидаемого {expected_contact_id}. Создание операции по отвязке.")
                        operations[
                            f"detach_contact_{deal_id}"] = f"crm.deal.contact.items.delete?ID={deal_id}&CONTACT_ID={contact_id}"
                    contact_id = expected_contact_id
                    logging.info(f"Контакт успешно перепривязан к ID {contact_id}.")

                # Удаление старой сделки
                if old_deal_id and old_deal_id['ID'] != deal_id:
                    logging.info(f"Удаление старой сделки ID {old_deal_id['ID']} для трек-номера {track_number}.")
                    operations[
                        f"detach_old_contact_{old_deal_id['ID']}"] = f"crm.deal.contact.items.delete?ID={old_deal_id['ID']}&CONTACT_ID={expected_contact_id}"
                    operations[f"delete_old_deal_{old_deal_id['ID']}"] = f"crm.deal.delete?id={old_deal_id['ID']}"

                # Маппинг для поля пункта выдачи
                pickup_mapping = {
                    "pv_karaganda_1": "52",
                    "pv_karaganda_2": "54",
                    "pv_astana_1": "48",
                    "pv_astana_2": "50"
                }
                pickup_point_mapped = pickup_mapping.get(client_info['pickup_point'])
                logging.info(f"Маппинг пункта выдачи для {client_info['pickup_point']}: {pickup_point_mapped}")

                # Обновление полей сделки
                title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
                logging.info(f"Обновление сделки ID {deal_id}: новый заголовок: {title}")
                operations[f"update_deal_{deal_id}"] = (
                    f"crm.deal.update?ID={deal_id}&fields[CONTACT_ID]={contact_id}&fields[TITLE]={title}"
                    f"&fields[PHONE]={client_info['phone']}&fields[CITY]={client_info['city']}"
                    f"&fields[UF_CRM_1723542556619]={track_number}&fields[UF_CRM_1723542922949]={pickup_point_mapped}"
                    f"&fields[UF_CRM_1725179625]={chat_id}"
                )
                logging.info(f"Операция обновления сделки добавлена для ID {deal_id}.")
                await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
            else:
                logging.warning(f"Клиент с chat_id {chat_id} не найден. Проверка завершена.")
        else:
            logging.info(f"Трек-номер {track_number} не найден в базе. Проверка завершена.")

    else:
        # Обработка для других категорий
        pipeline_stage = {
            0: 'ПВ Астана №1',
            2: 'ПВ Астана №2',
            4: 'ПВ Караганда №1',
            6: 'ПВ Караганда №2'
        }.get(int(category_id))

        logging.info(
            f"Начата обработка для других категорий. ID сделки: {deal_id}, трек-номер: {track_number}, категория: {category_id}")

        client_info = None
        if track_number:
            logging.info(f"Ищем данные по трек-номеру: {track_number}")
            track_data = get_track_data_by_track_number(track_number)
            if track_data:
                chat_id = track_data.get('chat_id')
                logging.info(f"Найдены данные по трек-номеру {track_number}: {track_data}")

                client_info = get_client_by_chat_id(chat_id)
                if client_info:
                    logging.info(f"Получены данные клиента по chat_id {chat_id}: {client_info}")

                    expected_contact_id = int(client_info.get('contact_id'))
                    old_deal_id = find_deal_by_track_number(track_number)
                    logging.info(f"Ожидаемый контакт ID: {expected_contact_id}. Найдено старое deal_id: {old_deal_id}")

                    # Проверка и перепривязка контакта
                    if contact_id != expected_contact_id:
                        logging.info(
                            f"Контакт ID {contact_id} отличается от ожидаемого {expected_contact_id}. Создание операции по отвязке.")
                        operations[
                            f"detach_contact_{deal_id}"] = f"crm.deal.contact.items.delete?ID={deal_id}&CONTACT_ID={contact_id}"
                        contact_id = expected_contact_id
                        logging.info(f"Контакт успешно перепривязан к ID {contact_id}.")

                    # Удаление старой сделки
                    if old_deal_id and old_deal_id['ID'] != deal_id:
                        logging.info(f"Удаление старой сделки ID {old_deal_id['ID']} для трек-номера {track_number}.")
                        operations[
                            f"detach_old_contact_{old_deal_id['ID']}"] = f"crm.deal.contact.items.delete?ID={old_deal_id['ID']}&CONTACT_ID={expected_contact_id}"
                        operations[f"delete_old_deal_{old_deal_id['ID']}"] = f"crm.deal.delete?id={old_deal_id['ID']}"

                    title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
                    logging.info(f"Обновление сделки ID {deal_id}: новый заголовок: {title}")
                    operations[f"update_deal_{deal_id}"] = (
                        f"crm.deal.update?ID={deal_id}&fields[CONTACT_ID]={contact_id}&fields[TITLE]={title}"
                        f"&fields[PHONE]={client_info['phone']}&fields[CITY]={client_info['city']}"
                        f"&fields[UF_CRM_1723542556619]={track_number}&fields[UF_CRM_1723542922949]={client_info['pickup_point']}"
                        f"&fields[UF_CRM_1725179625]={chat_id}"
                    )
                    await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
                else:
                    logging.warning(f"Клиент с chat_id {chat_id} не найден.")
            else:
                logging.info(f"Трек-номер {track_number} не найден в базе.")

        if not client_info and contact_id:
            logging.info(f"Попытка получения данных клиента по contact_id {contact_id}")
            client_info = get_client_by_contact_id(contact_id)

        if not client_info:
            logging.error(f"Клиентская информация не найдена для сделки {deal_id}. Пропуск обработки.")
            return

        logging.info(f"Получена информация о клиенте для создания/обновления итоговой сделки: {client_info}")

        today_date = datetime.now(timezone.utc).date()
        final_deal = get_final_deal_from_db(contact_id)
        logging.info(f"Проверяем наличие итоговой сделки для контакта ID {contact_id}. Найдено: {final_deal}")

        expected_awaiting_pickup_stage = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
        expected_issued_stage = stage_mapping.get(pipeline_stage, {}).get('issued')

        if final_deal:
            logging.info(f"Итоговая сделка найдена. Анализируем этапы и обновления.")
            final_deal_creation_date = datetime.strptime(final_deal['creation_date'], '%Y-%m-%d').date()
            current_stage_id = final_deal['current_stage_id']

            logging.info(
                f"Текущий этап итоговой сделки: {current_stage_id}, ожидаемый этап 'issued': {expected_issued_stage}, "
                f"ожидаемый этап 'awaiting_pickup': {expected_awaiting_pickup_stage}, дата создания: {final_deal_creation_date}, "
                f"сегодняшняя дата: {today_date}."
            )

            if current_stage_id == expected_issued_stage:
                logging.info(
                    f"Итоговая сделка для контакта {contact_id} находится на этапе 'issued' и не требует обновления.")

            elif final_deal_creation_date == today_date and current_stage_id == expected_awaiting_pickup_stage:
                logging.info(f"Итоговая сделка {final_deal['final_deal_id']} обновляется для контакта {contact_id}")
                current_track_numbers = final_deal['track_numbers']
                logging.info(f"Текущие трек-номера в итоговой сделке: {current_track_numbers}")
                updated_track_numbers = f"{current_track_numbers}, {track_number}".strip(
                    ', ') if current_track_numbers else track_number
                logging.info(f"Обновленные трек-номера для итоговой сделки: {updated_track_numbers}")
                operations[f"update_track_numbers_{final_deal['final_deal_id']}"] = (
                    f"crm.deal.update?id={final_deal['final_deal_id']}&fields[UF_CRM_1729115312]={updated_track_numbers}"
                )
                logging.info(f"Обновление трек-номеров в итоговой сделке: {updated_track_numbers}")
                archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
                operations[
                    f"archive_deal_{deal_id}"] = f"crm.deal.update?id={deal_id}&fields[STAGE_ID]={archive_stage_id}"
                update_final_deal_in_db(final_deal['final_deal_id'], updated_track_numbers, current_stage_id)
                logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
                delete_result = await delete_deal_by_track_number(track_number)

                if delete_result:
                    logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
                else:
                    logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")

            else:
                logging.warning(f"Условия для обновления или архивирования итоговой сделки не выполнены.")

        else:
            logging.info(f"Итоговая сделка отсутствует. Создание новой сделки.")
            pickup_mapping = {
                "pv_karaganda_1": "52",
                "pv_karaganda_2": "54",
                "pv_astana_1": "48",
                "pv_astana_2": "50"
            }
            operations[f"create_final_deal_{contact_id}"] = (
                f"crm.deal.add?fields[TITLE]=Итоговая сделка: {client_info['personal_code']} "
                f"{client_info['pickup_point']} {client_info['phone']}&fields[CONTACT_ID]={contact_id}&fields[STAGE_ID]={expected_awaiting_pickup_stage}"
                f"&fields[CATEGORY_ID]={category_id}&fields[UF_CRM_1723542922949]={pickup_mapping.get(client_info['pickup_point'])}"
                f"&fields[UF_CRM_1727870320443]={float(weight)}&fields[OPPORTUNITY]={float(amount)}&fields[UF_CRM_1730185262]={number_of_orders}"
                f"&fields[UF_CRM_1729115312]={track_number}&fields[UF_CRM_1729539412]=1&fields[OPENED]=Y"
            )
            logging.info(
                f"Создана операция для новой итоговой сделки. Трек-номер: {track_number}, контакт ID: {contact_id}")

            operations[f"update_contact_fields_{contact_id}"] = (
                f"crm.contact.update?id={contact_id}&fields[UF_CRM_1726207792191]={weight}"
                f"&fields[UF_CRM_1726207809637]={amount}&fields[UF_CRM_1730182877]={number_of_orders}"
            )
            archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
            operations[f"archive_deal_{deal_id}"] = f"crm.deal.update?id={deal_id}&fields[STAGE_ID]={archive_stage_id}"

            save_final_deal_to_db(
                contact_id=contact_id,
                deal_id=deal_id,
                creation_date=today_date.isoformat(),
                track_number=track_number,
                current_stage_id=expected_awaiting_pickup_stage,  # Передаем идентификатор из маппинга
                weight=weight,
                amount=amount,
                number_of_orders=number_of_orders
            )
            logging.info(f"Данные о новой итоговой сделке сохранены в базу данных.")


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

    # Данные контакта из CRM
    name_translit_crm = contact_info.get('UF_CRM_1730093824027')
    phone_crm = contact_info.get('PHONE', [{}])[0].get('VALUE', '')

    # Логируем данные для отладки
    logging.info(f"Данные из CRM - Имя: {name_translit_crm}, Телефон: {phone_crm}")
    logging.info(f"Данные из базы - Имя: {name_translit_db}, Телефон: {phone_db}")

    # Проверка на изменения
    if (name_translit_crm != name_translit_db) or (phone_crm != phone_db):
        logging.info(f"Обнаружены изменения в контактной информации для контакта {contact_id}. Уведомление не отправлено.")
    else:
        # Извлекаем значения пользовательских полей для уведомления
        weight = contact_info.get('UF_CRM_1726207792191')
        amount = contact_info.get('UF_CRM_1726207809637')
        number_of_orders = contact_info.get('UF_CRM_1730182877')

        # Отправляем уведомление только если поле amount заполнено и не равно нулю
        if amount and amount != '0':
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"⚖ Вес заказов: {weight} кг.\n"
                         f"💰 Сумма оплаты по весу: {amount} тг.\n"
                         f"📦 Количество заказов к выдаче: {number_of_orders}"
                )
                logging.info(f"Уведомление с весом и суммой отправлено пользователю с chat_id: {chat_id}")
            except Exception as e:
                logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
        else:
            logging.info("Поле 'Сумма заказов' не заполнено или равно нулю. Уведомление не отправлено.")
