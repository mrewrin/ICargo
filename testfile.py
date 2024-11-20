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

# копия main
# import logging
# import asyncio
# from fastapi import FastAPI, Request
# from aiogram import Bot, Dispatcher
# from aiogram.fsm.storage.memory import MemoryStorage
# from urllib.parse import parse_qs
# from datetime import datetime, timezone
# from config import bot_token
# from handlers import user_registration, user_update, menu_handling, track_management, \
#     package_search, information_instructions, settings
# from db_management import init_db, get_all_chat_ids, get_personal_code_by_chat_id, \
#     get_track_data_by_track_number, get_client_by_chat_id, is_vip_code_available, update_personal_code, \
#     remove_vip_code, get_contact_id_by_code, get_chat_id_by_contact_id, get_client_by_contact_id, \
#     delete_deal_by_track_number
# from bitrix_integration import get_contact_info, get_deal_info, find_deal_by_track_number, delete_deal, \
#     detach_contact_from_deal, update_standard_deal_fields, update_custom_deal_fields, \
#     update_contact_code_in_bitrix, find_final_deal_for_contact, update_final_deal, archive_deal, create_final_deal
# from aiogram.filters import Command
# from aiogram.types import Message, BotCommand, BotCommandScopeDefault, BotCommandScopeChat
#
# # Инициализация бота и роутера
# bot = Bot(token=bot_token)
# dp = Dispatcher(storage=MemoryStorage())
# dp.include_routers(user_registration.router, user_update.router, menu_handling.router, track_management.router,
#                    package_search.router, information_instructions.router, settings.router)
#
#
# # Список chat_id администраторов, которые могут отправлять рассылки
# ADMIN_IDS = [414935403]
#
# # Настройка логирования
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
#
# # Создание FastAPI приложения
# app = FastAPI()
#
# # Создаем очередь для задач
# deal_update_queue = asyncio.Queue(maxsize=100)
#
#
# # Установка команд для пользователей и админов
# async def set_bot_commands():
#     # Команды для всех пользователей
#     user_commands = [
#         BotCommand(command="/start", description="Начать диалог"),
#         BotCommand(command="/menu", description="Открыть меню"),
#         BotCommand(command="/clear", description="Очистить чат")
#     ]
#     await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())
#
#     # Команды для администраторов (включая команды для пользователей)
#     admin_commands = user_commands + [
#         BotCommand(command="/broadcast", description="Админ-рассылка (ввести /broadcast {текст рассылки})"),
#         BotCommand(command="/reappropriation", description="Переприсваивание VIP номера пользователю "
#                                                            "(ввести /reappropriation {старый_код} {новый_VIP_код})"),
#     ]
#     for admin_id in ADMIN_IDS:
#         await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))
#
#
# # Асинхронная блокировка для предотвращения гонок данных
# update_lock = asyncio.Lock()
#
#
# # Функция с логикой повторных попыток выполнения задачи
# async def retry(coro, max_retries=3, delay=2):
#     for attempt in range(max_retries):
#         try:
#             return await coro()
#         except Exception as e:
#             logging.error(f"Попытка {attempt + 1} не удалась: {e}")
#             await asyncio.sleep(delay)
#     logging.error(f"Задача провалилась после {max_retries} попыток.")
#     return False
#
#
# # Определение маппинга стадий для каждой воронки
# stage_mapping = {
#     'ПВ Астана №1': {
#         'arrived': 'NEW',
#         'awaiting_pickup': 'UC_MJZYDP',
#         'archive': 'LOSE',
#         'issued': 'WON'
#     },
#     'ПВ Астана №2': {
#         'arrived': 'C2:NEW',
#         'awaiting_pickup': 'C2:UC_8EQX6X',
#         'archive': 'C2:LOSE',
#         'issued': 'C2:WON'
#     },
#     'ПВ Караганда №1': {
#         'arrived': 'C4:NEW',
#         'awaiting_pickup': 'C4:UC_VOLZYJ',
#         'archive': 'C4:LOSE',
#         'issued': 'C4:WON'
#     },
#     'ПВ Караганда №2': {
#         'arrived': 'C6:NEW',
#         'awaiting_pickup': 'C6:UC_VEHS4L',
#         'archive': 'C6:LOSE',
#         'issued': 'C6:WON'
#     }
# }
#
#
# def get_pipeline_stage(category_id):
#     return {
#         0: 'ПВ Астана №1',
#         2: 'ПВ Астана №2',
#         4: 'ПВ Караганда №1',
#         6: 'ПВ Караганда №2'
#     }.get(category_id, 'ПВ Астана №1')  # Значение по умолчанию
#
#
# # Функция для обработки обновления сделок
# async def deal_update_worker():
#     logging.info("Воркер запущен и ожидает задачи.")
#     while True:
#         # Получаем сделку из очереди
#         logging.info("Ожидаем получение задачи из очереди")
#         deal_info = await deal_update_queue.get()
#         logging.info(f"Получена задача на обновление итоговой сделки: {deal_info}")
#
#         # Проверяем сигнал завершения
#         if deal_info is None:
#             logging.info("Получен сигнал завершения для воркера сделок.")
#             deal_update_queue.task_done()
#             break
#
#         # Разбираем данные сделки
#         deal_id = deal_info.get("deal_id")
#         track_number = deal_info.get("track_number")
#         category_id = deal_info.get("category_id")
#         deal_for_archive_id = deal_info.get("deal_for_archive_id")  # ID исходной сделки для архивирования
#
#         if not deal_id or not track_number:
#             logging.warning(f"Недостаточно данных для обновления сделки: deal_id={deal_id}, track_number={track_number}")
#             deal_update_queue.task_done()
#             continue
#
#         # Пример использования
#         pipeline_stage = get_pipeline_stage(category_id)
#
#         # Обновляем итоговую сделку с использованием механизма повторных попыток
#         async with update_lock:
#             success = await retry(lambda: update_final_deal(deal_id, track_number))
#
#         if success:
#             logging.info(f"Итоговая сделка {deal_id} успешно обновлена.")
#         else:
#             logging.error(f"Ошибка обновления итоговой сделки {deal_id}.")
#
#         # Асинхронный вызов функции перемещения текущей сделки в архив с передачей pipeline_stage
#         logging.info(f"Перемещаем сделку {deal_id} в архив с этапом {pipeline_stage}")
#         try:
#             archive_result = await retry(lambda: archive_deal(deal_for_archive_id, stage_mapping.get(pipeline_stage)))
#             if archive_result:
#                 logging.info(f"Сделка {deal_id} успешно перемещена в архив.")
#             else:
#                 logging.warning(f"Не удалось переместить сделку {deal_id} в архив.")
#         except Exception as e:
#             logging.error(f"Ошибка при перемещении сделки {deal_id} в архив: {e}")
#
#         # Удаление сделки из базы данных по трек-номеру с повторными попытками
#         logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
#         try:
#             delete_result = await retry(lambda: delete_deal_by_track_number(track_number))
#             if delete_result:
#                 logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
#             else:
#                 logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")
#         except Exception as e:
#             logging.error(f"Ошибка при удалении сделки с трек-номером {track_number}: {e}")
#
#         # Логируем текущий размер очереди для мониторинга
#         queue_size = deal_update_queue.qsize()
#         logging.info(f"Текущий размер очереди обновлений: {queue_size}")
#
#
# async def send_notification_if_required(deal_id, track_number, pickup_point):
#     """
#     Отправляет уведомление при успешном обновлении пользовательских полей.
#     """
#     # Получаем информацию о сделке
#     deal_info = await get_deal_info(deal_id)
#     if not deal_info:
#         logging.error(f"Не удалось получить информацию о сделке {deal_id} для отправки уведомления.")
#         return
#
#     stage_id = deal_info.get('STAGE_ID')
#     chat_id = deal_info.get('UF_CRM_1725179625')
#
#     # Определяем соответствующие пункты выдачи и стадии для уведомления
#     locations = {
#         '48': "г.Астана, ПВ №1",
#         '50': "г.Астана, ПВ №2",
#         '52': "г.Караганда, ПВ №1",
#         '54': "г.Караганда, ПВ №2"
#     }
#     status_code_list = {
#         "C4:NEW": "г.Караганда, ПВ №1",
#         "C6:NEW": "г.Караганда, ПВ №2",
#         "NEW": "г.Астана, ПВ №1",
#         "C2:NEW": "г.Астана, ПВ №2"
#     }
#     location_value = locations.get(deal_info.get('UF_CRM_1723542922949'), "неизвестное место выдачи")
#     stage_value = status_code_list.get(stage_id)
#     personal_code = get_personal_code_by_chat_id(chat_id)
#     logging.info(f"Проверка уведомления: стадия сделки={stage_id}, пункт выдачи={location_value}, "
#                  f"ожидаемая стадия={stage_value}, chat_id={chat_id}")
#
#     # Проверяем условия отправки уведомления
#     if location_value == stage_value and chat_id:
#         try:
#             message_text = f"Ваш заказ с трек номером {track_number} прибыл в пункт выдачи {location_value}."
#             if personal_code:
#                 message_text += f"\nВаш личный код: 讠AUG{personal_code}."
#             await bot.send_message(chat_id=chat_id, text=message_text)
#             logging.info(f"Уведомление отправлено пользователю с chat_id: {chat_id}")
#         except Exception as e:
#             logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
#     else:
#         logging.info(f"Уведомление не отправлено: стадия {stage_id} или локация {location_value} не соответствуют требуемым условиям.")
#
#
# # Асинхронный маршрут для обработки вебхуков от Bitrix
# @app.post("/webhook")
# async def handle_webhook(request: Request):
#     raw_body = await request.body()
#     decoded_body = parse_qs(raw_body.decode('utf-8'))
#     deal_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
#     logging.info(f"Received raw webhook data: {decoded_body}")
#
#     event_type = decoded_body.get('event', [''])[0]
#
#     # Обработка события обновления сделки
#     if event_type == 'ONCRMDEALUPDATE':
#         await process_deal_update(deal_id)
#     elif event_type == 'ONCRMDEALADD':
#         await process_deal_add(deal_id)
#     elif event_type == 'ONCRMCONTACTUPDATE':
#         contact_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
#         await process_contact_update(contact_id)
#
#
# async def process_deal_update(deal_id):
#     logging.info(f"Обработка события ONCRMDEALUPDATE для сделки с ID: {deal_id}")
#
#     # Получаем информацию о сделке
#     deal_info = await get_deal_info(deal_id)
#     if not deal_info:
#         logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")
#         return
#
#     # Извлечение ID контакта и трек-номера
#     stage_id = deal_info.get('STAGE_ID')
#     contact_id = deal_info.get('CONTACT_ID')
#     track_number = deal_info.get('UF_CRM_1723542556619', '')
#
#     # Проверяем, является ли сделка "итоговой"
#     is_final_deal = deal_info.get('UF_CRM_1729539412') == '1'
#     if is_final_deal:
#         logging.info(f"Сделка {deal_id} является итоговой, дальнейшая обработка не требуется.")
#         return
#
#     # Обрабатываем стадию "Выдан заказ"
#     if stage_id == 'WON':
#         logging.info(f"Стадия 'Выдан заказ' для сделки {deal_id}, контакт {contact_id}")
#
#         # Получаем данные контакта
#         contact_info = get_contact_info(contact_id)
#         if not contact_info:
#             logging.warning(f"Контакт с ID {contact_id} не найден.")
#             return
#
#         # Извлекаем данные о заказах
#         weight = contact_info.get('UF_CRM_1726207792191')
#         amount = contact_info.get('UF_CRM_1726207809637')
#         number_of_orders = contact_info.get('UF_CRM_1730182877')
#
#         # Ищем существующую итоговую сделку для контакта
#         final_deal = await find_final_deal_for_contact(contact_id, deal_id)
#         if final_deal:
#             logging.info(
#                 f"Добавляем задачу для обновления итоговой сделки с ID {final_deal['ID']} для контакта {contact_id}")
#             await deal_update_queue.put((final_deal['ID'], track_number))
#         else:
#             # Если итоговая сделка не найдена, создаем новую
#             chat_id = get_chat_id_by_contact_id(contact_id)
#             client_info = get_client_by_chat_id(chat_id)
#             if client_info:
#                 personal_code = client_info.get('personal_code')
#                 pickup_point = client_info.get('pickup_point')
#                 phone = client_info.get('phone')
#                 await create_final_deal(contact_id, weight, amount, number_of_orders, track_number, personal_code,
#                                         pickup_point, phone)
#             else:
#                 logging.warning(f"Информация о клиенте для chat_id {chat_id} не найдена.")
#
#         # Перемещаем текущую сделку в архив
#         logging.info(f"Перемещаем сделку {deal_id} в архив")
#         await archive_deal(deal_id)
#
#
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
#     # Проверяем, является ли сделка итоговой по флагу `UF_CRM_1729539412`
#     if deal_info.get('UF_CRM_1729539412') == '1':
#         logging.info(f"Сделка с ID {deal_id} является итоговой и не будет обработана.")
#         return
#
#     # Определяем этап сделки
#     stage_id = deal_info.get('STAGE_ID')
#     awaiting_pickup_stages = {v['awaiting_pickup'] for v in stage_mapping.values()}
#
#     # Игнорируем обработку для этапов `awaiting_pickup`
#     if stage_id in awaiting_pickup_stages:
#         logging.info(f"Сделка с ID {deal_id} находится на этапе 'awaiting_pickup' и не будет обработана.")
#         return
#
#     contact_id = deal_info.get('CONTACT_ID')
#     track_number = deal_info.get('UF_CRM_1723542556619', '')
#     category_id = deal_info.get('CATEGORY_ID')
#     weight = deal_info.get('UF_CRM_1729457411', 0)
#     amount = deal_info.get('UF_CRM_1729457446', 0)
#     number_of_orders = deal_info.get('UF_CRM_1730185262', 0)
#
#     logging.info(f"Полученные данные: contact_id={contact_id}, track_number={track_number}, "
#                  f"category_id={category_id}, weight={weight}, amount={amount}")
#     # Проверка категории сделки
#     if int(category_id) == 8:
#         logging.info("Категория сделки соответствует 8, выполняется альтернативная логика.")
#
#         # Если есть трек-номер, проверяем его в базе данных бота
#         if track_number:
#             logging.info(f"Проверяем наличие сделки с трек-номером {track_number}")
#             track_data = get_track_data_by_track_number(track_number)
#             logging.info(f"Результат поиска трек-номера {track_number} в базе: {track_data}")
#
#             if track_data:
#                 chat_id = track_data.get('chat_id')
#                 client_info = get_client_by_chat_id(chat_id)
#
#                 if client_info:
#                     expected_contact_id = client_info['contact_id']
#                     old_deal_id = find_deal_by_track_number(track_number)
#
#                     # Проверка текущего контакта и перепривязка при необходимости
#                     if contact_id != expected_contact_id:
#                         if contact_id:
#                             logging.info(
#                                 f"Сделка с ID {deal_id} привязана к некорректному контакту с ID {contact_id}. Перепривязываем к контакту с ID {expected_contact_id}.")
#                             detach_result = detach_contact_from_deal(deal_id, contact_id)
#                             if detach_result:
#                                 contact_id = expected_contact_id
#                         else:
#                             logging.info(
#                                 f"Сделка с ID {deal_id} не имеет привязанного контакта. Привязываем к контакту с ID {expected_contact_id}.")
#                             contact_id = expected_contact_id
#
#                     # Удаление старой сделки, если она найдена
#                     if old_deal_id and old_deal_id['ID'] != deal_id:
#                         logging.info(
#                             f"Обнаружена старая сделка с ID {old_deal_id['ID']} для контакта {contact_id}. Удаляем старую сделку.")
#                         detach_result = detach_contact_from_deal(old_deal_id['ID'], expected_contact_id)
#                         if detach_result:
#                             delete_result = delete_deal(old_deal_id['ID'])
#                             if delete_result:
#                                 logging.info(f"Старая сделка с ID {old_deal_id['ID']} успешно удалена.")
#                             else:
#                                 logging.error(f"Не удалось удалить старую сделку с ID {old_deal_id['ID']}.")
#                         else:
#                             logging.error(
#                                 f"Не удалось отвязать контакт с ID {expected_contact_id} от старой сделки {old_deal_id['ID']}.")
#
#                     # Обновление полей сделки и привязка нового контакта
#                     title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#                     update_standard_result = update_standard_deal_fields(deal_id, contact_id, title,
#                                                                          client_info['phone'], client_info['city'])
#                     update_custom_result = update_custom_deal_fields(deal_id, chat_id, track_number,
#                                                                      client_info['pickup_point'])
#
#                     # Уведомление при успешном обновлении полей сделки
#                     if update_standard_result and update_custom_result:
#                         logging.info(
#                             f"Контакт с ID {contact_id} успешно привязан и все поля сделки {deal_id} обновлены.")
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
#         logging.info(f'Категория для определения этапа {category_id}')
#         pipeline_stage = {
#             0: 'ПВ Астана №1',
#             2: 'ПВ Астана №2',
#             4: 'ПВ Караганда №1',
#             6: 'ПВ Караганда №2'
#         }.get(int(category_id))
#         logging.info(f'Этап для сделки: {pipeline_stage}')
#
#         # Определяем client_info для клиента на основе chat_id
#         client_info = None
#
#         # Если трек-номер указан, проверяем его в базе данных бота
#         if track_number:
#             track_data = get_track_data_by_track_number(track_number)
#             if not track_data:
#                 logging.warning(
#                     f"Трек-номер {track_number} отсутствует в базе данных, но продолжаем обработку сделки с имеющейся информацией о клиенте.")
#             else:
#                 chat_id = track_data.get('chat_id')
#                 client_info = get_client_by_chat_id(chat_id)
#                 logging.info(f'Информация о клиенте: {client_info}')
#                 expected_contact_id = int(client_info.get('contact_id')) if client_info else None
#
#                 # Проверка и перепривязка контакта при необходимости
#                 if contact_id:
#                     if int(contact_id) != expected_contact_id:
#                         logging.info(
#                             f"Сделка с ID {deal_id} привязана к некорректному контакту. Перепривязываем к контакту с ID {expected_contact_id}.")
#                         detach_contact_from_deal(deal_id, contact_id)
#                         contact_id = expected_contact_id
#                 else:
#                     contact_id = expected_contact_id
#                     logging.info(
#                         f"Сделка с ID {deal_id} не имела привязанного контакта. Привязываем контакт с ID {contact_id}.")
#
#                 # Формируем название сделки на основе данных клиента
#                 title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#
#                 # Обновляем стандартные поля сделки
#                 update_standard_result = update_standard_deal_fields(
#                     deal_id, contact_id=contact_id, title=title,
#                     phone=client_info['phone'], city=client_info['city']
#                 )
#                 update_custom_result = update_custom_deal_fields(
#
#                     deal_id, telegram_id=chat_id, track_number=track_number,
#                     pickup_point=client_info['pickup_point']
#                 )
#
#                 # Отправляем уведомление, если поля обновлены успешно
#                 if update_standard_result and update_custom_result:
#                     logging.info(
#                         f"Контакт с ID {contact_id} успешно привязан и все поля сделки {deal_id} обновлены.")
#                     await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
#                 else:
#                     logging.error(f"Не удалось обновить поля сделки {deal_id}.")
#
#         if not client_info and contact_id:
#             logging.info(f"Попытка получения данных о клиенте по contact_id {contact_id}")
#             client_info = get_client_by_contact_id(contact_id)
#             logging.info(f'Информация о клиенте: {client_info}')
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
#             expected_issued_stage = stage_mapping.get(pipeline_stage, {}).get('issued')
#
#             logging.info(f"pipeline_stage: {pipeline_stage}, current_stage_id: {current_stage_id}")
#             logging.info(
#                 f"expected_awaiting_pickup_stage: {expected_awaiting_pickup_stage}, expected_issued_stage: {expected_issued_stage}")
#
#             # Если сделка на этапе issued (например, WON), не трогаем её
#             if current_stage_id == expected_issued_stage:
#                 logging.info(
#                     f"Итоговая сделка для контакта {contact_id} находится на этапе 'issued' и не требует обновления.")
#             # Если сделка на этапе awaiting_pickup, обновляем её
#             elif final_deal_creation_date == today_date and current_stage_id == expected_awaiting_pickup_stage:
#                 logging.info(
#                     f"Итоговая сделка для контакта {contact_id} была создана сегодня и находится на этапе 'awaiting_pickup'. Обновляем её.")
#                 await deal_update_queue.put({
#                     "deal_id": final_deal['ID'],
#                     "track_number": track_number,
#                     "category_id": category_id,
#                     "deal_for_archive_id": deal_id
#                 })
#                 logging.info(
#                     f"Добавлена задача на обновление итоговой сделки: {final_deal['ID']} с трек-номером {track_number}")
#             else:
#                 # Если не найдено соответствие ни с одним из этапов, создаем новую итоговую сделку
#                 logging.info(
#                     f"Итоговая сделка для контакта {contact_id} не соответствует условиям обновления (дата или этап). Создаем новую.")
#                 logging.info(f"1Создаем итоговую сделку для контакта {contact_id} на этапе {pipeline_stage}")
#                 await create_final_deal(
#                     contact_id=contact_id,
#                     weight=weight,
#                     amount=amount,
#                     number_of_orders=number_of_orders,
#                     track_number=track_number,
#                     personal_code=client_info['personal_code'],
#                     pickup_point=client_info['pickup_point'],
#                     phone=client_info['phone'],
#                     pipeline_stage=pipeline_stage,
#                     category_id=category_id
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
#             logging.info(f"2Создаем итоговую сделку для контакта {contact_id} на этапе {pipeline_stage}")
#             await create_final_deal(
#                 contact_id=contact_id,
#                 weight=weight,
#                 amount=amount,
#                 number_of_orders=number_of_orders,
#                 track_number=track_number,
#                 personal_code=client_info['personal_code'],
#                 pickup_point=client_info['pickup_point'],
#                 phone=client_info['phone'],
#                 pipeline_stage=pipeline_stage,
#                 category_id=category_id
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
#
#
# async def process_contact_update(contact_id):
#     logging.info(f"Обработка события ONCRMCONTACTUPDATE для контакта с ID: {contact_id}")
#
#     # # Маппинг локаций
#     # locations = {
#     #     '48': "г.Астана, ПВ №1",
#     #     '50': "г.Астана, ПВ №2",
#     #     '52': "г.Караганда, ПВ №1",
#     #     '54': "г.Караганда, ПВ №2"
#     # }
#     #
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
#     # Получаем данные из локальной базы данных
#     client_data = get_client_by_chat_id(chat_id)
#     if not client_data:
#         logging.warning(f"Данные клиента для chat_id {chat_id} не найдены.")
#         return
#
#     # Извлекаем данные для сравнения
#     name_translit_db = client_data['name_translit']
#     phone_db = client_data['phone']
#     # pickup_point_db = client_data['pickup_point']
#
#     name_translit_crm = contact_info.get('UF_CRM_1730093824027')
#     phone_crm = contact_info.get('PHONE', [{}])[0].get('VALUE', '')
#     # pickup_point_crm_code = contact_info.get('UF_CRM_1723542816833')
#     # pickup_point_crm = locations.get(pickup_point_crm_code, '')
#
#     # Логируем данные для отладки
#     logging.info(f"Данные из CRM - Имя: {name_translit_crm}, Телефон: {phone_crm}")
#     logging.info(f"Данные из базы - Имя: {name_translit_db}, Телефон: {phone_db}")
#
#     # Проверка на изменения
#     if (name_translit_crm != name_translit_db) or (phone_crm != phone_db):
#         logging.info(f"Обнаружены изменения в контактной информации для контакта {contact_id}. Уведомление не отправлено.")
#     else:
#         # Извлекаем значения пользовательских полей для уведомления
#         weight = contact_info.get('UF_CRM_1726207792191')
#         amount = contact_info.get('UF_CRM_1726207809637')
#         number_of_orders = contact_info.get('UF_CRM_1730182877')
#
#         # Отправляем уведомление только если поле amount заполнено и не равно нулю
#         if amount and amount != '0':
#             try:
#                 await bot.send_message(
#                     chat_id=chat_id,
#                     text=f"⚖ Вес заказов: {weight} кг.\n"
#                          f"💰 Сумма оплаты по весу: {amount} тг.\n"
#                          f"📦 Количество заказов к выдаче: {number_of_orders}"
#                 )
#                 logging.info(f"Уведомление с весом и суммой отправлено пользователю с chat_id: {chat_id}")
#             except Exception as e:
#                 logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
#         else:
#             logging.info("Поле 'Сумма заказов' не заполнено или равно нулю. Уведомление не отправлено.")

#
# # Команда для рассылки сообщений всем пользователям
# @dp.message(Command("broadcast"))
# async def broadcast_message(message: Message):
#     if message.from_user.id not in ADMIN_IDS:
#         await message.answer("У вас нет прав для выполнения этой команды.")
#         return
#
#     # Получаем полный текст сообщения
#     message_text = message.text
#
#     # Убираем команду "/broadcast" из текста сообщения
#     if message_text.startswith("/broadcast"):
#         broadcast_text = message_text[len("/broadcast"):].strip()
#     else:
#         broadcast_text = ""
#
#     if not broadcast_text:
#         await message.answer("Пожалуйста, укажите сообщение для рассылки.")
#         return
#
#     # Получаем все chat_id из базы данных
#     chat_ids = get_all_chat_ids()
#
#     # Рассылка сообщений
#     for chat_id in chat_ids:
#         try:
#             await bot.send_message(chat_id=chat_id, text=broadcast_text)
#             logging.info(f"Сообщение отправлено пользователю {chat_id}")
#         except Exception as e:
#             logging.error(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")
#
#     await message.answer("Рассылка завершена.")
#
#
# # Команда для изменения персонального кода на VIP
# @dp.message(Command("reappropriation"))
# async def reappropriation(message: Message):
#     if message.from_user.id not in ADMIN_IDS:
#         await message.answer("У вас нет прав для выполнения этой команды.")
#         return
#
#     # Получаем полный текст сообщения
#     message_text = message.text
#
#     # Убираем команду "/reappropriation" из текста сообщения
#     if message_text.startswith("/reappropriation"):
#         args = message_text[len("/reappropriation"):].strip()
#     else:
#         args = ""
#
#     # Разбиваем аргументы на старый и новый код
#     args = args.split()
#     if len(args) != 2:
#         await message.answer("Пожалуйста, укажите текущий и новый VIP код клиента. "
#                              "Пример: /reappropriation {старый_код} {новый_VIP_код}")
#         return
#
#     old_code, new_code = args
#
#     # Получаем contact_id по старому коду перед обновлением
#     contact_id = get_contact_id_by_code(old_code)
#     logging.info(f"Получен contact_id: {contact_id}")
#
#     # Проверяем, существует ли новый VIP код в базе данных
#     if not is_vip_code_available(new_code):
#         await message.answer(f"VIP код {new_code} недоступен или уже присвоен другому пользователю.")
#         return
#
#     # Обновляем personal_code пользователя
#     if update_personal_code(old_code, new_code):
#         # Если contact_id найден, обновляем код в Bitrix
#         if contact_id:
#             logging.info('update_contact_code_in_bitrix called')
#             update_contact_code_in_bitrix(contact_id, new_code)
#
#         # Удаляем использованный VIP код из базы данных
#         remove_vip_code(new_code)
#         await message.answer(f"Клиенту с кодом {old_code} присвоен новый VIP код {new_code}.")
#     else:
#         await message.answer(f"Ошибка: пользователь с кодом {old_code} не найден или произошла ошибка при обновлении.")
#
#
# # Функция для запуска всех сервисов
# async def start_services():
#     logging.info("Запуск всех сервисов...")
#
#     # Устанавливаем команды для бота
#     await set_bot_commands()
#
#     # Запускаем несколько воркеров для обработки сделок (например, 3 воркера)
#     worker_tasks = [asyncio.create_task(deal_update_worker()) for _ in range(3)]
#     logging.info(f"Запущено {len(worker_tasks)} воркеров для обработки сделок.")
#
#     # Запуск FastAPI сервера
#     import uvicorn
#     config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="info")
#     server = uvicorn.Server(config)
#
#     # Запускаем сервер и бота параллельно
#     await asyncio.gather(server.serve(), dp.start_polling(bot))
#
#     # Убеждаемся, что все задачи в очереди завершены перед завершением воркеров
#     await deal_update_queue.join()
#     logging.info("Все задачи в очереди обработаны. Завершаем воркеры.")
#
#     # Корректно завершаем все воркеры после завершения работы
#     for _ in worker_tasks:
#         await deal_update_queue.put(None)  # Сигналы для остановки всех воркеров
#     await asyncio.gather(*worker_tasks)  # Ожидание завершения всех воркеров
#
#     logging.info("Сервисы корректно завершены.")
#
#
# def run_bot_and_server():
#     init_db()  # Инициализация базы данных
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(start_services())  # Запуск всех сервисов
#
#
# # Запуск приложения
# if __name__ == '__main__':
#     run_bot_and_server()





#
# async def process_deal_add(deal_id):
#     logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")
#
#     # Получаем информацию о сделке
#     deal_info = await get_deal_info(deal_id)
#     if not deal_info:
#         logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")
#         return
#
#     # Проверка, является ли сделка итоговой
#     if deal_info.get('UF_CRM_1729539412') == '1':
#         logging.info(f"Сделка с ID {deal_id} является итоговой и не будет обработана.")
#         return
#
#     # Этап и категория
#     stage_id = deal_info.get('STAGE_ID')
#     category_id = deal_info.get('CATEGORY_ID')
#     awaiting_pickup_stages = {v['awaiting_pickup'] for v in stage_mapping.values()}
#
#     if stage_id in awaiting_pickup_stages:
#         logging.info(f"Сделка с ID {deal_id} находится на этапе 'awaiting_pickup' и не будет обработана.")
#         return
#
#     contact_id = deal_info.get('CONTACT_ID')
#     track_number = deal_info.get('UF_CRM_1723542556619', '')
#     weight = deal_info.get('UF_CRM_1729457411', 0)
#     amount = deal_info.get('UF_CRM_1729457446', 0)
#     number_of_orders = deal_info.get('UF_CRM_1730185262', 0)
#
#     # Инициализируем накопитель операций
#     operations = {}
#
#     # Логика для альтернативной категории 8
#     if int(category_id) == 8:
#         if track_number:
#             track_data = get_track_data_by_track_number(track_number)
#             if track_data:
#                 chat_id = track_data.get('chat_id')
#                 client_info = get_client_by_chat_id(chat_id)
#                 if client_info:
#                     expected_contact_id = client_info['contact_id']
#                     old_deal_id = find_deal_by_track_number(track_number)
#
#                     # Проверка и перепривязка контакта
#                     if contact_id != expected_contact_id:
#                         if contact_id:
#                             operations['detach_contact'] = {
#                                 'method': 'crm.deal.contact.items.delete',
#                                 'params': {'ID': deal_id, 'CONTACT_ID': contact_id}
#                             }
#                         contact_id = expected_contact_id
#
#                     # Удаление старой сделки
#                     if old_deal_id and old_deal_id['ID'] != deal_id:
#                         operations['detach_old_contact'] = {
#                             'method': 'crm.deal.contact.items.delete',
#                             'params': {'ID': old_deal_id['ID'], 'CONTACT_ID': expected_contact_id}
#                         }
#                         operations['delete_old_deal'] = {
#                             'method': 'crm.deal.delete',
#                             'params': {'id': old_deal_id['ID']}
#                         }
#
#                     # Обновление полей сделки
#                     title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#                     operations['update_deal'] = {
#                         'method': 'crm.deal.update',
#                         'params': {
#                             'ID': deal_id,
#                             'fields': {
#                                 'CONTACT_ID': contact_id,
#                                 'TITLE': title,
#                                 'PHONE': client_info['phone'],
#                                 'CITY': client_info['city'],
#                                 'UF_CRM_1723542556619': track_number,
#                                 'UF_CRM_1723542922949': client_info['pickup_point'],
#                                 'UF_CRM_1725179625': chat_id
#                             }
#                         }
#                     }
#                     await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
#
#                 else:
#                     logging.warning(f"Клиент с chat_id {chat_id} не найден.")
#             else:
#                 logging.info(f"Трек-номер {track_number} не найден в базе.")
#
#     else:
#         pipeline_stage = {
#             0: 'ПВ Астана №1',
#             2: 'ПВ Астана №2',
#             4: 'ПВ Караганда №1',
#             6: 'ПВ Караганда №2'
#         }.get(int(category_id))
#
#         client_info = None
#         if track_number:
#             track_data = get_track_data_by_track_number(track_number)
#             if track_data:
#                 chat_id = track_data.get('chat_id')
#                 client_info = get_client_by_chat_id(chat_id)
#                 if client_info:
#                     expected_contact_id = int(client_info.get('contact_id'))
#                     if contact_id != expected_contact_id:
#                         operations['detach_contact'] = {
#                             'method': 'crm.deal.contact.items.delete',
#                             'params': {'ID': deal_id, 'CONTACT_ID': contact_id}
#                         }
#                         contact_id = expected_contact_id
#
#                     title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
#                     operations['update_deal'] = {
#                         'method': 'crm.deal.update',
#                         'params': {
#                             'ID': deal_id,
#                             'fields': {
#                                 'CONTACT_ID': contact_id,
#                                 'TITLE': title,
#                                 'PHONE': client_info['phone'],
#                                 'CITY': client_info['city'],
#                                 'UF_CRM_1723542556619': track_number,
#                                 'UF_CRM_1723542922949': client_info['pickup_point'],
#                                 'UF_CRM_1725179625': chat_id
#                             }
#                         }
#                     }
#                     await send_notification_if_required(deal_id, track_number, client_info['pickup_point'])
#
#         if not client_info and contact_id:
#             client_info = get_client_by_contact_id(contact_id)
#
#         today_date = datetime.now(timezone.utc).date()
#         final_deal = await find_final_deal_for_contact(contact_id, exclude_deal_id=deal_id)
#
#         if final_deal:
#             final_deal_creation_date = datetime.strptime(final_deal.get('DATE_CREATE')[:10], '%Y-%m-%d').date()
#             current_stage_id = final_deal.get('STAGE_ID')
#             expected_awaiting_pickup_stage = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup')
#             expected_issued_stage = stage_mapping.get(pipeline_stage, {}).get('issued')
#
#             if current_stage_id == expected_issued_stage:
#                 logging.info(f"Итоговая сделка для контакта {contact_id} на этапе 'issued' и не требует обновления.")
#             elif final_deal_creation_date == today_date and current_stage_id == expected_awaiting_pickup_stage:
#                 logging.info(f"Итоговая сделка {final_deal['ID']} обновляется для контакта {contact_id}")
#
#                 # Обновление трек-номера
#                 current_track_numbers = final_deal.get('UF_CRM_1729115312', '')
#                 updated_track_numbers = f"{current_track_numbers}, {track_number}".strip(', ') if current_track_numbers else track_number
#                 operations['update_track_numbers'] = {
#                     'method': 'crm.deal.update',
#                     'params': {'id': final_deal['ID'], 'fields': {'UF_CRM_1729115312': updated_track_numbers}}
#                 }
#
#                 # Архивирование текущей сделки
#                 archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
#                 operations['archive_deal'] = {
#                     'method': 'crm.deal.update',
#                     'params': {'id': deal_id, 'fields': {'STAGE_ID': archive_stage_id}}
#                 }
#
#                 # Удаление трек-номера из базы
#                 logging.info(f"Попытка удаления сделки с трек-номером {track_number} из базы данных.")
#                 delete_result = await delete_deal_by_track_number(track_number)
#                 if delete_result:
#                     logging.info(f"Сделка с трек-номером {track_number} успешно удалена из базы данных.")
#                 else:
#                     logging.warning(f"Сделка с трек-номером {track_number} не найдена или уже была удалена.")
#             else:
#                 await create_final_deal(
#                     contact_id=contact_id, weight=weight, amount=amount,
#                     number_of_orders=number_of_orders, track_number=track_number,
#                     personal_code=client_info['personal_code'], pickup_point=client_info['pickup_point'],
#                     phone=client_info['phone'], pipeline_stage=pipeline_stage, category_id=category_id
#                 )
#                 archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
#                 operations['archive_deal'] = {
#                     'method': 'crm.deal.update',
#                     'params': {'id': deal_id, 'fields': {'STAGE_ID': archive_stage_id}}
#                 }
#                 await delete_deal_by_track_number(track_number)
#         else:
#             await create_final_deal(
#                 contact_id=contact_id, weight=weight, amount=amount,
#                 number_of_orders=number_of_orders, track_number=track_number,
#                 personal_code=client_info['personal_code'], pickup_point=client_info['pickup_point'],
#                 phone=client_info['phone'], pipeline_stage=pipeline_stage, category_id=category_id
#             )
#             archive_stage_id = stage_mapping.get(pipeline_stage, {}).get('archive', 'LOSE')
#             operations['archive_deal'] = {
#                 'method': 'crm.deal.update',
#                 'params': {'id': deal_id, 'fields': {'STAGE_ID': archive_stage_id}}
#             }
#             await delete_deal_by_track_number(track_number)
#
#     # Выполняем все накопленные операции через call_batch
#     if operations:
#         response = await bitrix.call_batch(operations)
#         if 'error' in response:
#             logging.error(f"Ошибка при выполнении batch операций: {response['error_description']}")
#         else:
#             logging.info(f"Batch операции успешно выполнены: {response}")
#
#             # Определение маппинга стадий для каждой воронки
#             stage_mapping = {
#                 'ПВ Астана №1': {
#                     'arrived': 'NEW',
#                     'awaiting_pickup': 'UC_MJZYDP',
#                     'archive': 'LOSE',
#                     'issued': 'WON'
#                 },
#                 'ПВ Астана №2': {
#                     'arrived': 'C2:NEW',
#                     'awaiting_pickup': 'C2:UC_8EQX6X',
#                     'archive': 'C2:LOSE',
#                     'issued': 'C2:WON'
#                 },
#                 'ПВ Караганда №1': {
#                     'arrived': 'C4:NEW',
#                     'awaiting_pickup': 'C4:UC_VOLZYJ',
#                     'archive': 'C4:LOSE',
#                     'issued': 'C4:WON'
#                 },
#                 'ПВ Караганда №2': {
#                     'arrived': 'C6:NEW',
#                     'awaiting_pickup': 'C6:UC_VEHS4L',
#                     'archive': 'C6:LOSE',
#                     'issued': 'C6:WON'
#                 }
#             }
#
#             def get_pipeline_stage(category_id):
#                 return {
#                     0: 'ПВ Астана №1',
#                     2: 'ПВ Астана №2',
#                     4: 'ПВ Караганда №1',
#                     6: 'ПВ Караганда №2'
#                 }.get(category_id, 'ПВ Астана №1')  # Значение по умолчанию


