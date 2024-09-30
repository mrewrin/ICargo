import logging
import asyncio
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from urllib.parse import parse_qs
from config import bot_token
from handlers import user_registration, user_update, menu_handling, track_management, \
    package_search, information_instructions, settings
from db_management import init_db, get_all_chat_ids, get_personal_code_by_chat_id, \
    get_track_data_by_track_number, get_client_by_chat_id, is_vip_code_available, update_personal_code, \
    remove_vip_code, get_contact_id_by_code, get_chat_id_by_contact_id
from bitrix_integration import get_contact_info, get_deal_info, find_deal_by_track_number, delete_deal, \
    detach_contact_from_deal, update_standard_deal_fields, update_custom_deal_fields, \
    update_contact_code_in_bitrix, update_contact_fields_in_bitrix
from aiogram.filters import Command
from aiogram.types import Message, BotCommand, BotCommandScopeDefault, BotCommandScopeChat

# Инициализация бота и роутера
bot = Bot(token=bot_token)
dp = Dispatcher(storage=MemoryStorage())
dp.include_routers(user_registration.router, user_update.router, menu_handling.router, track_management.router,
                   package_search.router, information_instructions.router, settings.router)

# Список chat_id администраторов, которые могут отправлять рассылки
ADMIN_IDS = [414935403]

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

# Создание FastAPI приложения
app = FastAPI()


# Установка команд для пользователей и админов
async def set_bot_commands():
    # Команды для всех пользователей
    user_commands = [
        BotCommand(command="/start", description="Начать диалог"),
        BotCommand(command="/menu", description="Открыть меню"),
        BotCommand(command="/clear", description="Очистить чат")
    ]
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    # Команды для администраторов (включая команды для пользователей)
    admin_commands = user_commands + [
        BotCommand(command="/broadcast", description="Админ-рассылка (ввести /broadcast {текст рассылки})"),
        BotCommand(command="/reappropriation", description="Переприсваивание VIP номера пользователю "
                                                           "(ввести /reappropriation {старый_код} {новый_VIP_код})"),
    ]
    for admin_id in ADMIN_IDS:
        await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))


# Асинхронный маршрут для обработки вебхуков от Bitrix
@app.post("/webhook")
async def handle_webhook(request: Request):
    raw_body = await request.body()
    decoded_body = parse_qs(raw_body.decode('utf-8'))
    deal_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
    contact_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
    logging.info(f"Received raw webhook data: {decoded_body}")

    # Определенные стадии, по которым нужно отправлять уведомление
    status_code_list = {
        "C4:NEW": "г.Караганда, ПВ №1",
        "C6:NEW": "г.Караганда, ПВ №2",
        "NEW": "г.Астана, ПВ №1",
        "C2:NEW": "г.Астана, ПВ №2"
    }

    # Обработка события ONCRMDEALUPDATE
    if decoded_body.get('event', [''])[0] == 'ONCRMDEALUPDATE':
        deal_info = get_deal_info(deal_id)
        logging.info(f'Deal Info: {deal_info}')
        if deal_info:
            stage_id = deal_info.get('STAGE_ID')  # Получаем текущую стадию сделки
            if stage_id in status_code_list:
                # Уведомление будет отправлено только для стадий из status_code_list
                chat_id = deal_info.get('UF_CRM_1725179625')
                track_number = deal_info.get('UF_CRM_1723542556619')
                p_point = deal_info.get('UF_CRM_1723542922949')
                locations = {
                    '48': "г.Астана, ПВ №1",
                    '50': "г.Астана, ПВ №2",
                    '52': "г.Караганда, ПВ №1",
                    '54': "г.Караганда, ПВ №2"
                }
                location_value = locations.get(p_point, "неизвестное место выдачи")
                stage_value = status_code_list.get(stage_id)
                # Получаем personal_code по chat_id
                personal_code = get_personal_code_by_chat_id(chat_id)
                if location_value == stage_value:
                    if chat_id:
                        try:
                            # Включаем personal_code в сообщение, если он найден
                            if personal_code:
                                await bot.send_message(chat_id=chat_id,
                                                       text=f"Ваш заказ с трек номером {track_number} "
                                                            f"прибыл в пункт выдачи {location_value}.\n"
                                                            f"Ваш личный код: 讠AUG{personal_code}.")
                            else:
                                await bot.send_message(chat_id=chat_id, text=f"Ваш заказ с трек номером {track_number} "
                                                                             f"прибыл в пункт выдачи {location_value}.")
                            logging.info(f"Уведомление отправлено пользователю с chat_id: {chat_id}")
                        except Exception as e:
                            logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. "
                                          f"Ошибка: {e}")
            else:
                logging.info(f"Стадия {stage_id} не требует отправки уведомления.")
        else:
            logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")

    # Обработка события ONCRMDEALADD
    elif decoded_body.get('event', [''])[0] == 'ONCRMDEALADD':
        # Получаем данные о сделке
        logging.info(f"Обработка события ONCRMDEALADD для сделки с ID: {deal_id}")
        deal_info = get_deal_info(deal_id)
        logging.info(f'Получена информация о сделке: {deal_info}')

        if deal_info:
            contact_id = deal_info.get('CONTACT_ID')
            track_number = deal_info.get('UF_CRM_1723542556619')

            logging.info(f"Полученные данные: contact_id={contact_id}, track_number={track_number}")

            # Если contact_id отсутствует
            if not contact_id and track_number:
                logging.info(
                    f"Сделка с ID {deal_id} не имеет привязанного контакта, ищем по трек-номеру {track_number}")

                # Проверяем, существует ли такой трек-номер в базе
                track_data = get_track_data_by_track_number(track_number)
                logging.info(f"Результат поиска трек-номера {track_number} в базе: {track_data}")

                if track_data:
                    # Получаем chat_id и информацию о клиенте по трек-номеру
                    chat_id = track_data.get('chat_id')
                    logging.info(f"Найден chat_id: {chat_id} по трек-номеру {track_number}")
                    telegram_id = chat_id

                    client_info = get_client_by_chat_id(chat_id)
                    logging.info(f"Информация о клиенте для chat_id {chat_id}: {client_info}")

                    if client_info:
                        contact_id = client_info['contact_id']
                        logging.info(f"Найден contact_id {contact_id} для клиента {chat_id}")

                        # Получаем старую сделку с таким же трек-номером
                        old_deal_id = find_deal_by_track_number(track_number)
                        logging.info(f"Найдена старая сделка с таким трек-номером: {old_deal_id}")

                        if old_deal_id:
                            logging.info(f"Отвязываем контакт с ID {contact_id} от старой сделки с ID {old_deal_id}.")
                            detach_result = detach_contact_from_deal(old_deal_id['ID'], contact_id)
                            if detach_result:
                                logging.info(f"Контакт с ID {contact_id} успешно отвязан от сделки {old_deal_id}.")
                                delete_result = delete_deal(old_deal_id['ID'])
                                if delete_result:
                                    logging.info(f"Старая сделка с ID {old_deal_id} успешно удалена.")
                                else:
                                    logging.error(f"Не удалось удалить старую сделку с ID {old_deal_id}.")
                            else:
                                logging.error(f"Не удалось отвязать контакт с ID {contact_id} от сделки {old_deal_id}.")

                        # Обновляем новую сделку: стандартные поля
                        title = f"{client_info['personal_code']} {client_info['pickup_point']} {client_info['phone']}"
                        update_standard_result = update_standard_deal_fields(deal_id, contact_id, title,
                                                                             client_info['phone'], client_info['city'])

                        # Обновляем пользовательские поля
                        update_custom_result = update_custom_deal_fields(deal_id, telegram_id,
                                                                         track_number, client_info['pickup_point'])

                        if update_standard_result and update_custom_result:
                            logging.info(
                                f"Контакт с ID {contact_id} успешно привязан и все поля сделки {deal_id} обновлены.")
                        else:
                            logging.error(f"Не удалось обновить поля сделки {deal_id}.")

                    else:
                        logging.warning(f"Клиент с chat_id {chat_id} не найден.")
                else:
                    logging.info(f"Трек-номер {track_number} не найден в базе.")
            else:
                logging.info(f"Сделка с ID {deal_id} уже привязана к контакту с ID {contact_id}.")
        else:
            logging.warning(f"Информация о сделке с ID {deal_id} не найдена.")

    # Обработка события ONCRMCONTACTUPDATE
    elif decoded_body.get('event', [''])[0] == 'ONCRMCONTACTUPDATE':
        # Получаем данные о контакте
        logging.info(f"Обработка события ONCRMCONTACTUPDATE для контакта с ID: {contact_id}")
        contact_info = get_contact_info(contact_id)
        logging.info(f'Получена информация о контакте: {contact_info}')

        if contact_info:
            # Получаем значения пользовательских полей
            weight = contact_info.get('UF_CRM_1726207792191')
            amount = contact_info.get('UF_CRM_1726207809637')
            total_weight = contact_info.get('UF_CRM_1726837773968')
            total_amount = contact_info.get('UF_CRM_1726837761251')

            # Проверяем, что сумма заказов заполнена и не равна нулю
            if amount and amount != '0':
                # Получаем chat_id по contact_id
                chat_id = get_chat_id_by_contact_id(contact_id)

                if chat_id:
                    try:
                        # Отправляем уведомление пользователю
                        await bot.send_message(chat_id=chat_id, text=f"📦 Вес заказов: {weight} кг.\n"
                                                                     f"💰 Сумма оплаты по весу: {amount} тг.")
                        logging.info(f"Уведомление с весом и суммой отправлено пользователю с chat_id: {chat_id}")

                        # Прибавляем значения weight и amount к total_weight и total_amount
                        sum_weight = float(total_weight or 0) + float(weight or 0)
                        sum_amount = float(total_amount or 0) + float(amount or 0)

                        # Обновляем пользовательские поля контакта
                        update_contact_fields_in_bitrix(contact_id, sum_weight, sum_amount)

                    except Exception as e:
                        logging.error(f"Ошибка при отправке сообщения пользователю с chat_id: {chat_id}. Ошибка: {e}")
            else:
                logging.info("Поле 'Сумма заказов' не заполнено или равно нулю. Уведомление не отправлено.")
        else:
            logging.warning(f"Информация о контакте с ID {contact_id} не найдена.")


# Команда для рассылки сообщений всем пользователям
@dp.message(Command("broadcast"))
async def broadcast_message(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    # Получаем полный текст сообщения
    message_text = message.text

    # Убираем команду "/broadcast" из текста сообщения
    if message_text.startswith("/broadcast"):
        broadcast_text = message_text[len("/broadcast"):].strip()
    else:
        broadcast_text = ""

    if not broadcast_text:
        await message.answer("Пожалуйста, укажите сообщение для рассылки.")
        return

    # Получаем все chat_id из базы данных
    chat_ids = get_all_chat_ids()

    # Рассылка сообщений
    for chat_id in chat_ids:
        try:
            await bot.send_message(chat_id=chat_id, text=broadcast_text)
            logging.info(f"Сообщение отправлено пользователю {chat_id}")
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")

    await message.answer("Рассылка завершена.")


# Команда для изменения персонального кода на VIP
# Команда для изменения персонального кода на VIP
@dp.message(Command("reappropriation"))
async def reappropriation(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    # Получаем полный текст сообщения
    message_text = message.text

    # Убираем команду "/reappropriation" из текста сообщения
    if message_text.startswith("/reappropriation"):
        args = message_text[len("/reappropriation"):].strip()
    else:
        args = ""

    # Разбиваем аргументы на старый и новый код
    args = args.split()
    if len(args) != 2:
        await message.answer("Пожалуйста, укажите текущий и новый VIP код клиента. "
                             "Пример: /reappropriation {старый_код} {новый_VIP_код}")
        return

    old_code, new_code = args

    # Получаем contact_id по старому коду перед обновлением
    contact_id = get_contact_id_by_code(old_code)
    logging.info(f"Получен contact_id: {contact_id}")

    # Проверяем, существует ли новый VIP код в базе данных
    if not is_vip_code_available(new_code):
        await message.answer(f"VIP код {new_code} недоступен или уже присвоен другому пользователю.")
        return

    # Обновляем personal_code пользователя
    if update_personal_code(old_code, new_code):
        # Если contact_id найден, обновляем код в Bitrix
        if contact_id:
            logging.info('update_contact_code_in_bitrix called')
            update_contact_code_in_bitrix(contact_id, new_code)

        # Удаляем использованный VIP код из базы данных
        remove_vip_code(new_code)
        await message.answer(f"Клиенту с кодом {old_code} присвоен новый VIP код {new_code}.")
    else:
        await message.answer(f"Ошибка: пользователь с кодом {old_code} не найден или произошла ошибка при обновлении.")


# Функция запуска aiogram и FastAPI вместе
async def start_services():
    # Устанавливаем команды для бота
    await set_bot_commands()

    # Запуск FastAPI сервера
    import uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await asyncio.gather(server.serve(), dp.start_polling(bot))


def run_bot_and_server():
    init_db()  # Инициализация базы данных
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_services())


# Запуск приложения
if __name__ == '__main__':
    run_bot_and_server()
