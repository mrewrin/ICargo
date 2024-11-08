# async def process_deal_add(deal_id):
#     logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")
#
#     # Получаем информацию о сделке
#     deal_info = await get_deal_info(deal_id)
#     logging.info(deal_info)
#     if not deal_info:
#         logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")
#         return
#
#     contact_id = deal_info.get('CONTACT_ID')
#     track_number = deal_info.get('UF_CRM_1723542556619', '')
#     category_id = deal_info.get('CATEGORY_ID')
#
#     # Получаем информацию о контакте для извлечения weight, amount и number_of_orders
#     contact_info = get_contact_info(contact_id)
#     if not contact_info:
#         logging.warning(f"Информация о контакте с ID {contact_id} не найдена.")
#         return
#
#     # Извлекаем значения weight, amount, и number_of_orders из контактной информации
#     weight = contact_info.get('UF_CRM_1726207792191', 0)  # Примерное поле для weight
#     amount = contact_info.get('UF_CRM_1726207809637', 0)  # Примерное поле для amount
#     number_of_orders = contact_info.get('UF_CRM_1730182877', 0)  # Примерное поле для количества заказов
#
#     logging.info(f"Полученные данные: contact_id={contact_id}, track_number={track_number}, "
#                  f"category_id={category_id}, weight={weight}, amount={amount}")
#     # Проверка категории сделки
#     if int(category_id) == 8:
#         logging.info("Категория сделки соответствует 8, выполняется альтернативная логика.")
#
#         # Если contact_id отсутствует и есть трек-номер
#         if not contact_id and track_number:
#             logging.info(f"Сделка с ID {deal_id} не имеет привязанного контакта, ищем по трек-номеру {track_number}")
#             track_data = get_track_data_by_track_number(track_number)
#             logging.info(f"Результат поиска трек-номера {track_number} в базе: {track_data}")
#
#             if track_data:
#                 chat_id = track_data.get('chat_id')
#                 client_info = get_client_by_chat_id(chat_id)
#                 if client_info:
#                     contact_id = client_info['contact_id']
#                     old_deal_id = find_deal_by_track_number(track_number)
#
#                     # Отвязка старой сделки и удаление, если найдено
#                     if old_deal_id:
#                         logging.info(f"Отвязываем контакт с ID {contact_id} от старой сделки с ID {old_deal_id}.")
#                         detach_result = detach_contact_from_deal(old_deal_id['ID'], contact_id)
#                         if detach_result:
#                             delete_result = delete_deal(old_deal_id['ID'])
#                             if delete_result:
#                                 logging.info(f"Старая сделка с ID {old_deal_id} успешно удалена.")
#                             else:
#                                 logging.error(f"Не удалось удалить старую сделку с ID {old_deal_id}.")
#                         else:
#                             logging.error(f"Не удалось отвязать контакт с ID {contact_id} от сделки {old_deal_id}.")
#
#                     # Обновление новой сделки
#                     title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#                     update_standard_result = update_standard_deal_fields(deal_id, contact_id, title, client_info['phone'], client_info['city'])
#                     update_custom_result = update_custom_deal_fields(deal_id, chat_id, track_number, client_info['pickup_point'])
#
#                     if update_standard_result and update_custom_result:
#                         logging.info(f"Контакт с ID {contact_id} успешно привязан и все поля сделки {deal_id} обновлены.")
#                         await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
#                     else:
#                         logging.error(f"Не удалось обновить поля сделки {deal_id}.")
#                 else:
#                     logging.warning(f"Клиент с chat_id {chat_id} не найден.")
#             else:
#                 logging.info(f"Трек-номер {track_number} не найден в базе.")
#         else:
#             logging.info(f"Сделка с ID {deal_id} уже привязана к контакту с ID {contact_id}.")
#     else:
#         # Определяем pipeline_stage на основе CATEGORY_ID
#         pipeline_stage = {
#             0: 'ПВ Астана №1',
#             2: 'ПВ Астана №2',
#             4: 'ПВ Караганда №1',
#             6: 'ПВ Караганда №2'
#         }.get(category_id, 'ПВ Астана №1')
#
#         # Определяем client_info для клиента на основе chat_id
#         client_info = None
#
#         # Если трек-номер указан, проверяем его в базе данных бота
#         if track_number:
#             track_data = get_track_data_by_track_number(track_number)
#             if not track_data:
#                 logging.warning(f"Трек-номер {track_number} отсутствует в базе данных, но продолжаем обработку сделки с имеющейся информацией о клиенте.")
#             else:
#                 chat_id = track_data.get('chat_id')
#                 client_info = get_client_by_chat_id(chat_id)
#                 expected_contact_id = client_info.get('contact_id') if client_info else None
#
#                 if contact_id:
#                     # Если контакт привязан, проверяем совпадение с ожидаемым контактом
#                     if contact_id == expected_contact_id:
#                         logging.info(f"Сделка с ID {deal_id} уже привязана к корректному контакту с ID {contact_id}. Продолжаем обработку.")
#                     else:
#                         logging.info(f"Сделка с ID {deal_id} привязана к некорректному контакту. Отвязываем контакт с ID {contact_id} и привязываем контакт с ID {expected_contact_id}.")
#                         detach_contact_from_deal(deal_id, contact_id)
#                         contact_id = expected_contact_id
#                         update_standard_deal_fields(deal_id, contact_id, client_info['personal_code'], client_info['phone'], client_info['city'])
#                         logging.info(f"Контакт с ID {expected_contact_id} успешно привязан к сделке {deal_id}.")
#                 else:
#                     contact_id = expected_contact_id
#                     logging.info(f"Сделка с ID {deal_id} не имела привязанного контакта. Привязываем контакт с ID {contact_id}.")
#                     update_standard_deal_fields(deal_id, contact_id, client_info['personal_code'], client_info['phone'], client_info['city'])
#
#                 update_custom_deal_fields(deal_id, chat_id, track_number, client_info['pickup_point'])
#
#         if not client_info and contact_id:
#             logging.info(f"Попытка получения данных о клиенте по contact_id {contact_id}")
#             client_info = get_client_by_contact_id(contact_id)
#
#         # Обработка итоговой сделки
#         today_date = datetime.now(timezone.utc).date()
#         final_deal = await find_final_deal_for_contact(contact_id, exclude_deal_id=deal_id)
#         logging.info(f"Результат поиска итоговой сделки для контакта {contact_id}: {final_deal}")
#
#         if final_deal:
#             final_deal_creation_date_str = final_deal.get('DATE_CREATE')
#             final_deal_creation_date = datetime.strptime(final_deal_creation_date_str[:10], '%Y-%m-%d').date()
#
#             current_stage_id = final_deal.get('STAGE_ID')
#             expected_awaiting_pickup_stage = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
#
#             if final_deal_creation_date == today_date and current_stage_id == expected_awaiting_pickup_stage:
#                 logging.info(f"Итоговая сделка для контакта {contact_id} была создана сегодня и находится на этапе 'awaiting_pickup'. Обновляем её.")
#                 await deal_update_queue.put({
#                     "deal_id": final_deal['ID'],
#                     "track_number": track_number,
#                     "category_id": category_id,
#                     "deal_for_archive_id": deal_id
#                 })
#                 logging.info(f"Добавлена задача на обновление итоговой сделки: {final_deal['ID']} с трек-номером {track_number}")
#             else:
#                 logging.info(f"Итоговая сделка для контакта {contact_id} не соответствует условиям обновления (дата или этап). Создаем новую.")
#                 await create_final_deal(
#                     contact_id=contact_id,
#                     weight=weight,
#                     amount=amount,
#                     number_of_orders=number_of_orders,
#                     track_number=track_number,
#                     personal_code=client_info['personal_code'],
#                     pickup_point=client_info['pickup_point'],
#                     phone=client_info['phone'],
#                     pipeline_stage=pipeline_stage
#                 )
#                 await archive_deal(deal_id, stage_mapping.get(pipeline_stage))
#                 logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
#                 try:
#                     delete_result = await retry(lambda: delete_deal_by_track_number(track_number))
#                     if delete_result:
#                         logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
#                     else:
#                         logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")
#                 except Exception as e:
#                     logging.error(f"Ошибка при удалении сделки с трек-номером {track_number}: {e}")
#         else:
#             logging.info(f"Создаем итоговую сделку для контакта {contact_id}")
#             await create_final_deal(
#                 contact_id=contact_id,
#                 weight=weight,
#                 amount=amount,
#                 number_of_orders=number_of_orders,
#                 track_number=track_number,
#                 personal_code=client_info['personal_code'],
#                 pickup_point=client_info['pickup_point'],
#                 phone=client_info['phone'],
#                 pipeline_stage=pipeline_stage
#             )
#             await archive_deal(deal_id, stage_mapping.get(pipeline_stage))
#             logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
#             try:
#                 delete_result = await retry(lambda: delete_deal_by_track_number(track_number))
#                 if delete_result:
#                     logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
#                 else:
#                     logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")
#             except Exception as e:
#                 logging.error(f"Ошибка при удалении сделки с трек-номером {track_number}: {e}")







# # Асинхронный маршрут для обработки вебхуков от Bitrix
# @app.post("/webhook")
# async def handle_webhook(request: Request):
#     raw_body = await request.body()
#     decoded_body = parse_qs(raw_body.decode('utf-8'))
#     deal_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
#     contact_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
#     logging.info(f"Received raw webhook data: {decoded_body}")
#
#     # Обработка события ONCRMDEALUPDATE
#     if decoded_body.get('event', [''])[0] == 'ONCRMDEALUPDATE':
#         logging.info(f"Обработка события ONCRMDEALUPDATE для сделки с ID: {deal_id}")
#         # Асинхронный вызов функции получения информации о сделке
#         deal_info = await get_deal_info(deal_id)
#         logging.info(f'Deal Info: {deal_info}')
#         logging.info(f"Track number: {deal_info.get('UF_CRM_1723542556619', '')}")
#
#         if deal_info:
#             stage_id = deal_info.get('STAGE_ID')  # Получаем текущую стадию сделки
#             contact_id = deal_info.get('CONTACT_ID')  # Получаем ID контакта
#
#             # Проверяем, является ли сделка "итоговой"
#             is_final_deal = deal_info.get('UF_CRM_1729539412') == '1'
#             if is_final_deal:
#                 logging.info(f"Сделка {deal_id} является итоговой, дальнейшая обработка не требуется.")
#                 return
#
#             if stage_id == 'WON':  # Стадия "Выдан заказ"
#                 logging.info(f"Стадия 'Выдан заказ' для сделки {deal_id}, контакт {contact_id}")
#
#                 # Асинхронный вызов функции получения информации о контакте
#                 contact_info = get_contact_info(contact_id)
#                 if contact_info:
#                     weight = contact_info.get('UF_CRM_1726207792191')
#                     amount = contact_info.get('UF_CRM_1726207809637')
#                     number_of_orders = contact_info.get('UF_CRM_1730182877')
#                     track_number = deal_info.get('UF_CRM_1723542556619', '')
#
#                     # Поиск итоговой сделки для контакта
#                     final_deal = await find_final_deal_for_contact(contact_id, deal_id)
#                     if final_deal:
#                         # Добавляем задачу в очередь для обновления существующей итоговой сделки
#                         logging.info(
#                             f"Добавляем задачу для обновления итоговой сделки с ID {final_deal['ID']}
#                             для контакта {contact_id}")
#                         await deal_update_queue.put((final_deal['ID'], track_number))
#                     else:
#                         # Создаем новую итоговую сделку
#                         chat_id = get_chat_id_by_contact_id(contact_id)
#                         client_info = get_client_by_chat_id(chat_id)
#                         if client_info:
#                             personal_code = client_info.get('personal_code')
#                             pickup_point = client_info.get('pickup_point')
#                             phone = client_info.get('phone')
#                             await create_final_deal(contact_id, weight, amount, number_of_orders, track_number,
#                                                     personal_code, pickup_point, phone)
#                         else:
#                             logging.warning(f"Информация о клиенте для chat_id {chat_id} не найдена.")
#
#                     # Асинхронный вызов функции перемещения текущей сделки в архив
#                     logging.info(f"Перемещаем сделку {deal_id} в архив")
#                     await archive_deal(deal_id)
#
#                 else:
#                     logging.warning(f"Контакт с ID {contact_id} не найден.")
#         else:
#             logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")
#
#     # Обработка события ONCRMDEALADD
#     elif decoded_body.get('event', [''])[0] == 'ONCRMDEALADD':
#         # Получаем данные о сделке
#         logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")
#         deal_info = await get_deal_info(deal_id)
#         logging.info(f'Получена информация о сделке: {deal_info}')
#
#         if deal_info:
#             contact_id = deal_info.get('CONTACT_ID')
#             track_number = deal_info.get('UF_CRM_1723542556619')
#
#             logging.info(f"Полученные данные: contact_id={contact_id}, track_number={track_number}")
#
#             # Если contact_id отсутствует
#             if not contact_id and track_number:
#                 logging.info(
#                     f"Сделка с ID {deal_id} не имеет привязанного контакта, ищем по трек-номеру {track_number}")
#
#                 # Проверяем, существует ли такой трек-номер в базе
#                 track_data = get_track_data_by_track_number(track_number)
#                 logging.info(f"Результат поиска трек-номера {track_number} в базе: {track_data}")
#
#                 if track_data:
#                     # Получаем chat_id и информацию о клиенте по трек-номеру
#                     chat_id = track_data.get('chat_id')
#                     logging.info(f"Найден chat_id: {chat_id} по трек-номеру {track_number}")
#                     telegram_id = chat_id
#
#                     client_info = get_client_by_chat_id(chat_id)
#                     logging.info(f"Информация о клиенте для chat_id {chat_id}: {client_info}")
#
#                     if client_info:
#                         contact_id = client_info['contact_id']
#                         logging.info(f"Найден contact_id {contact_id} для клиента {chat_id}")
#
#                         # Получаем старую сделку с таким же трек-номером
#                         old_deal_id = find_deal_by_track_number(track_number)
#                         logging.info(f"Найдена старая сделка с таким трек-номером: {old_deal_id}")
#
#                         if old_deal_id:
#                             logging.info(f"Отвязываем контакт с ID {contact_id} от старой сделки с ID {old_deal_id}.")
#                             detach_result = detach_contact_from_deal(old_deal_id['ID'], contact_id)
#                             if detach_result:
#                                 logging.info(f"Контакт с ID {contact_id} успешно отвязан от сделки {old_deal_id}.")
#                                 delete_result = delete_deal(old_deal_id['ID'])
#                                 if delete_result:
#                                     logging.info(f"Старая сделка с ID {old_deal_id} успешно удалена.")
#                                 else:
#                                     logging.error(f"Не удалось удалить старую сделку с ID {old_deal_id}.")
#                             else:
#                                 logging.error(f"Не удалось отвязать контакт с ID {contact_id} от сделки {old_deal_id}.")
#
#                         # Обновляем новую сделку: стандартные поля
#                         title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#                         update_standard_result = update_standard_deal_fields(deal_id, contact_id, title,
#                                                                              client_info['phone'], client_info['city'])
#
#                         # Обновляем пользовательские поля
#                         update_custom_result = update_custom_deal_fields(deal_id, telegram_id,
#                                                                          track_number, client_info['pickup_point'])
#
#                         if update_standard_result and update_custom_result:
#                             logging.info(
#                                 f"Контакт с ID {contact_id} успешно привязан и все поля сделки {deal_id} обновлены.")
#                             await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
#                         else:
#                             logging.error(f"Не удалось обновить поля сделки {deal_id}.")
#
#                     else:
#                         logging.warning(f"Клиент с chat_id {chat_id} не найден.")
#                 else:
#                     logging.info(f"Трек-номер {track_number} не найден в базе.")
#             else:
#                 logging.info(f"Сделка с ID {deal_id} уже привязана к контакту с ID {contact_id}.")
#         else:
#             logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")
#
#     # Обработка события ONCRMCONTACTUPDATE
#     elif decoded_body.get('event', [''])[0] == 'ONCRMCONTACTUPDATE':
#         # Получаем данные о контакте
#         logging.info(f"Обработка события ONCRMCONTACTUPDATE для контакта с ID: {contact_id}")
#         contact_info = get_contact_info(contact_id)
#         logging.info(f'Получена информация о контакте: {contact_info}')
#
#         if contact_info:
#             # Получаем значения пользовательских полей
#             weight = contact_info.get('UF_CRM_1726207792191')
#             amount = contact_info.get('UF_CRM_1726207809637')
#             number_of_orders = contact_info.get('UF_CRM_1730182877')
#             total_weight = contact_info.get('UF_CRM_1726837773968')
#             total_amount = contact_info.get('UF_CRM_1726837761251')
#
#             # Проверяем, что сумма заказов заполнена и не равна нулю
#             if amount and amount != '0':
#                 # Получаем chat_id по contact_id
#                 chat_id = get_chat_id_by_contact_id(contact_id)
#
#                 if chat_id:
#                     try:
#                         # Отправляем уведомление пользователю
#                         await bot.send_message(chat_id=chat_id, text=f"⚖ Вес заказов: {weight} кг.\n"
#                                                                      f"💰 Сумма оплаты по весу: {amount} тг.\n"
#                                                                      f"📦 Количество заказов к выдаче: {number_of_orders}")
#                         logging.info(f"Уведомление с весом и суммой отправлено пользователю с chat_id: {chat_id}")
#                     except Exception as e:
#                         logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
#             else:
#                 logging.info("Поле 'Сумма заказов' не заполнено или равно нулю. Уведомление не отправлено.")
#         else:
#             logging.warning(f"Информация о контакте с ID {contact_id} не найдена.")

# # Для очередей на уведомление о весе и сумме
# import asyncio
# from datetime import datetime, timedelta
#
# # Пример кэша для хранения статуса отправки (можно использовать базу данных)
# notification_cache = {}
#
# async def process_contact_update(contact_id):
#     logging.info(f"Обработка события ONCRMCONTACTUPDATE для контакта с ID: {contact_id}")
#
#     # Получаем данные о контакте из CRM
#     contact_info = get_contact_info(contact_id)
#     if not contact_info:
#         logging.warning(f"Информация о контакте с ID {contact_id} не найдена.")
#         return
#
#     # Получаем chat_id по contact_id
#     chat_id = get_chat_id_by_contact_id(contact_id)
#     if not chat_id:
#         logging.warning(f"chat_id для контакта {contact_id} не найден.")
#         return
#
#     # Проверяем статус уведомления и устанавливаем задержку на 30 минут
#     if chat_id in notification_cache:
#         last_notification_time = notification_cache[chat_id]
#         if datetime.now() < last_notification_time + timedelta(minutes=30):
#             logging.info(f"Уведомление для chat_id {chat_id} уже ожидает отправки.")
#             return
#     else:
#         notification_cache[chat_id] = datetime.now()
#
#     # Извлекаем значения полей контакта для уведомления
#     weight = contact_info.get('UF_CRM_1726207792191')
#     amount = contact_info.get('UF_CRM_1726207809637')
#     number_of_orders = contact_info.get('UF_CRM_1730182877')
#
#     # Устанавливаем отложенную отправку уведомления на 30 минут
#     async def delayed_notification():
#         await asyncio.sleep(1800)  # Задержка в 30 минут
#         try:
#             await bot.send_message(
#                 chat_id=chat_id,
#                 text=f"⚖ Вес заказов: {weight} кг.\n"
#                      f"💰 Сумма оплаты по весу: {amount} тг.\n"
#                      f"📦 Количество заказов к выдаче: {number_of_orders}"
#             )
#             logging.info(f"Уведомление с весом и суммой отправлено пользователю с chat_id: {chat_id}")
#             notification_cache.pop(chat_id, None)  # Удаляем запись после успешной отправки
#         except Exception as e:
#             logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
#
#     # Проверяем, что поле amount заполнено для уведомления, и запускаем отложенную задачу
#     if amount and amount != '0':
#         asyncio.create_task(delayed_notification())
#     else:
#         logging.info("Поле 'Сумма заказов' не заполнено или равно нулю. Уведомление не отправлено.")

category_id = 2

pipeline_stage = {
    0: 'ПВ Астана №1',
    2: 'ПВ Астана №2',
    4: 'ПВ Караганда №1',
    6: 'ПВ Караганда №2'
}.get(category_id)

print(pipeline_stage)

# # Проверка категории сделки
# if int(category_id) == 8:
#     logging.info("Категория сделки соответствует 8, выполняется альтернативная логика.")
#
#     # Если contact_id отсутствует и есть трек-номер
#     if not contact_id and track_number:
#         logging.info(f"Сделка с ID {deal_id} не имеет привязанного контакта, ищем по трек-номеру {track_number}")
#         track_data = get_track_data_by_track_number(track_number)
#         logging.info(f"Результат поиска трек-номера {track_number} в базе: {track_data}")
#
#         if track_data:
#             chat_id = track_data.get('chat_id')
#             client_info = get_client_by_chat_id(chat_id)
#             if client_info:
#                 contact_id = client_info['contact_id']
#                 old_deal_id = find_deal_by_track_number(track_number)
#
#                 # Отвязка старой сделки и удаление, если найдено
#                 if old_deal_id:
#                     logging.info(f"Отвязываем контакт с ID {contact_id} от старой сделки с ID {old_deal_id}.")
#                     detach_result = detach_contact_from_deal(old_deal_id['ID'], contact_id)
#                     if detach_result:
#                         delete_result = delete_deal(old_deal_id['ID'])
#                         if delete_result:
#                             logging.info(f"Старая сделка с ID {old_deal_id} успешно удалена.")
#                         else:
#                             logging.error(f"Не удалось удалить старую сделку с ID {old_deal_id}.")
#                     else:
#                         logging.error(f"Не удалось отвязать контакт с ID {contact_id} от сделки {old_deal_id}.")
#
#                 # Обновление новой сделки
#                 title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#                 update_standard_result = update_standard_deal_fields(deal_id, contact_id, title, client_info['phone'],
#                                                                      client_info['city'])
#                 update_custom_result = update_custom_deal_fields(deal_id, chat_id, track_number,
#                                                                  client_info['pickup_point'])
#
#                 if update_standard_result and update_custom_result:
#                     logging.info(f"Контакт с ID {contact_id} успешно привязан и все поля сделки {deal_id} обновлены.")
#                     await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
#                 else:
#                     logging.error(f"Не удалось обновить поля сделки {deal_id}.")
#             else:
#                 logging.warning(f"Клиент с chat_id {chat_id} не найден.")
#         else:
#             logging.info(f"Трек-номер {track_number} не найден в базе.")
#     else:
#         logging.info(f"Сделка с ID {deal_id} уже привязана к контакту с ID {contact_id}.")


# else:
# # Определяем pipeline_stage на основе CATEGORY_ID
# logging.info(f'Категория для определения этапа {category_id}')
# pipeline_stage = {
#     0: 'ПВ Астана №1',
#     2: 'ПВ Астана №2',
#     4: 'ПВ Караганда №1',
#     6: 'ПВ Караганда №2'
# }.get(int(category_id))
# logging.info(f'Этап для сделки: {pipeline_stage}')
# # Определяем client_info для клиента на основе chat_id
# client_info = None
#
# # Если трек-номер указан, проверяем его в базе данных бота
# if track_number:
#     track_data = get_track_data_by_track_number(track_number)
#     if not track_data:
#         logging.warning(
#             f"Трек-номер {track_number} отсутствует в базе данных, но продолжаем обработку сделки с имеющейся информацией о клиенте.")
#     else:
#         chat_id = track_data.get('chat_id')
#         client_info = get_client_by_chat_id(chat_id)
#         logging.info(f'Информация о клиенте: {client_info}')
#         expected_contact_id = client_info.get('contact_id') if client_info else None
#
#         if contact_id:
#             # Если контакт привязан, проверяем совпадение с ожидаемым контактом
#             if int(contact_id) == int(expected_contact_id):
#                 logging.info(
#                     f"Сделка с ID {deal_id} уже привязана к корректному контакту с ID {contact_id}. Продолжаем обработку.")
#             else:
#                 logging.info(
#                     f"Сделка с ID {deal_id} привязана к некорректному контакту. Отвязываем контакт с ID {contact_id} и привязываем контакт с ID {expected_contact_id}.")
#                 detach_contact_from_deal(deal_id, contact_id)
#                 contact_id = expected_contact_id
#                 update_standard_deal_fields(deal_id, contact_id, client_info['personal_code'], client_info['phone'],
#                                             client_info['city'])
#                 logging.info(f"Контакт с ID {expected_contact_id} успешно привязан к сделке {deal_id}.")
#         else:
#             contact_id = expected_contact_id
#             logging.info(
#                 f"Сделка с ID {deal_id} не имела привязанного контакта. Привязываем контакт с ID {contact_id}.")
#             update_standard_deal_fields(deal_id, contact_id, client_info['personal_code'], client_info['phone'],
#                                         client_info['city'])
#
#         update_custom_deal_fields(deal_id, chat_id, track_number, client_info['pickup_point'])
#         # Отправляем уведомление, если поля обновлены успешно
#         if client_info and chat_id:
#             await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
