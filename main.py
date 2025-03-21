import logging
import asyncio
from fastapi import FastAPI, Request
from aiogram import Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from urllib.parse import parse_qs
from datetime import datetime, timedelta

from bot_instance import bot
from handlers import user_registration, user_update, menu_handling, track_management, \
    package_search, information_instructions, settings
from batch_processing import batch_send_to_bitrix
from db_management import init_db, get_all_chat_ids, is_vip_code_available, update_personal_code, \
    remove_vip_code, get_contact_id_by_code, save_webhook_to_db, save_broadcast_message, get_last_broadcast_messages, \
    get_unprocessed_webhooks, is_code_used_by_another_client, get_chat_id_by_personal_code, \
    delete_deal_by_track_number, delete_client_from_db, get_all_final_deals_by_contact_id, delete_final_deal_from_db
from bitrix_integration import update_contact_code_in_bitrix, get_deal_info, get_deals_by_track_ident, delete_deal
from aiogram.filters import Command
from aiogram.types import Message, BotCommand, BotCommandScopeDefault, BotCommandScopeChat, FSInputFile
from functions import export_database_to_excel


# ========== Инициализация бота и приложения ==========

# bot = Bot(token=bot_token)
dp = Dispatcher(storage=MemoryStorage())
dp.include_routers(track_management.router,  # Специфические обработчики
                   package_search.router,   # Обработчики поиска
                   user_registration.router, # Регистрация пользователя
                   user_update.router,      # Обновление данных пользователя
                   menu_handling.router,    # Общие обработчики
                   information_instructions.router,  # Инструкции
                   settings.router)         # Настройки


app = FastAPI()

# ========== Настройки и конфигурация ==========

ADMIN_IDS = [379337072, 793398371, 7184969628, 414935403]
CHECK_INTERVAL = 10
IDLE_THRESHOLD = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")


# ========== Установка команд для бота ==========

async def set_bot_commands():
    # Устанавливаем команды для пользователей
    user_commands = [
        BotCommand(command="/start", description="Начать диалог"),
        BotCommand(command="/menu", description="Открыть меню")
    ]
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    # Устанавливаем команды для администраторов
    admin_commands = user_commands + [
        BotCommand(command="/broadcast", description="Админ-рассылка (ввести /broadcast {текст рассылки})"),
        BotCommand(command="/delete_broadcast", description="Удалить последнее сообщение рассылки"),
        BotCommand(command="/edit_broadcast", description="Изменить последнее сообщение рассылки "
                                                          "(ввести /edit_broadcast {новый текст рассылки})"),
        BotCommand(command="/reappropriation", description="Переприсваивание VIP номера пользователю "
                                                           "(ввести /reappropriation {старый_код} {новый_VIP_код})"),
        BotCommand(command="/delete_track", description="Удалить сделку или трек-номер из базы и Битрикс "
                                                        "(ввести /delete_track deal {deal_id} "
                                                        "или /delete_track number {track_number})"),
        BotCommand(command="/delete_client", description="Удалить клиента по номеру телефона "
                                                         "(ввести /delete_client {номер_телефона})"),
        BotCommand(command="/export_db", description="Выгрузить базу данных в Excel"),
        BotCommand(command="/get_final_deals", description="Получить итоговые сделки по contact_id, "
                                                           "(ввести /get_final_deals {ID контакта из битрикс})"),
        BotCommand(command="/delete_final_deal", description="Удалить итоговую сделку по final_deal_id"
                                                             "(ввести /delete_final_deal {ID итоговой сделки из битрикс})"),
    ]
    for admin_id in ADMIN_IDS:
        await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))


# ========== Вебхук таймер и обработка ==========

async def webhook_timer():
    """
    Таймер для периодической проверки необработанных вебхуков.
    Запускает обработку, если есть необработанные вебхуки и с момента последнего вебхука прошло достаточно времени.
    """
    while True:
        await asyncio.sleep(CHECK_INTERVAL)

        unprocessed_webhooks = get_unprocessed_webhooks()
        if unprocessed_webhooks:
            last_webhook_time = datetime.fromisoformat(unprocessed_webhooks[-1]["timestamp"])
            if (datetime.utcnow() - last_webhook_time).total_seconds() > IDLE_THRESHOLD:
                logging.info("Простой вебхуков зафиксирован. Запуск обработки...")
                await batch_send_to_bitrix()
            else:
                logging.info("Новые вебхуки продолжают поступать. Ожидаем простоя для обработки.")


# Асинхронный маршрут для обработки вебхуков от Bitrix
@app.post("/icargo/webhook")
async def handle_webhook(request: Request):
    raw_body = await request.body()
    decoded_body = parse_qs(raw_body.decode('utf-8'))
    event_type = decoded_body.get('event', [''])[0]
    entity_id = decoded_body.get('data[FIELDS][ID]', [''])[0]
    logging.info(f"Received webhook: event_type={event_type}, entity_id={entity_id}")
    save_webhook_to_db(entity_id, event_type)
    return {"status": "Webhook received and saved"}


# ========== Команды администратора ==========

@dp.message(Command("broadcast"))
async def broadcast_message(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    # Извлекаем текст сообщения для рассылки
    message_text = message.text[len("/broadcast"):].strip() if message.text.startswith("/broadcast") else ""
    if not message_text:
        await message.answer("Пожалуйста, укажите сообщение для рассылки.")
        return

    # Получаем список всех chat_id для рассылки
    chat_ids = get_all_chat_ids()  # Предполагается, что функция возвращает список chat_id

    # Отправляем сообщение каждому пользователю
    for chat_id in chat_ids:
        try:
            sent_message = await message.bot.send_message(chat_id=chat_id, text=message_text)
            logging.info(f"Сообщение отправлено пользователю {chat_id}")

            # Сохраняем данные об отправленном сообщении в базу данных
            save_broadcast_message(chat_id=chat_id, message_id=sent_message.message_id)

        except Exception as e:
            logging.error(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")

    await message.answer("Рассылка завершена.")


@dp.message(Command("delete_broadcast"))
async def delete_last_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    messages = get_last_broadcast_messages()
    if not messages:
        await message.answer("Нет сообщений для удаления.")
        return

    for chat_id, message_id in messages:
        try:
            await message.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logging.info(f"Сообщение {message_id} удалено для пользователя {chat_id}")
        except Exception as e:
            logging.error(f"Не удалось удалить сообщение {message_id} для пользователя {chat_id}: {e}")

    await message.answer("Последняя рассылка успешно удалена.")


@dp.message(Command("edit_broadcast"))
async def edit_last_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    args = message.text[len("/edit_broadcast"):].strip()
    if not args:
        await message.answer("Пожалуйста, укажите новый текст сообщения.")
        return

    new_text = args
    messages = get_last_broadcast_messages()
    if not messages:
        await message.answer("Нет сообщений для редактирования.")
        return

    for chat_id, message_id in messages:
        try:
            await message.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=new_text)
            logging.info(f"Сообщение {message_id} для пользователя {chat_id} обновлено.")
        except Exception as e:
            logging.error(f"Не удалось обновить сообщение {message_id} для пользователя {chat_id}: {e}")

    await message.answer("Последняя рассылка успешно отредактирована.")


@dp.message(Command("reappropriation"))
async def reappropriation(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    args = message.text[len("/reappropriation"):].strip().split() if message.text.startswith("/reappropriation") else []
    if len(args) != 2:
        await message.answer(
            "Пожалуйста, укажите текущий и новый VIP код клиента. \n"
            "Пример: /reappropriation {старый_код} {новый_VIP_код}"
        )
        return

    old_code, new_code = args
    contact_id = get_contact_id_by_code(old_code)
    logging.info(f"Получен contact_id: {contact_id}")

    # Проверяем, есть ли новый код в базе VIP кодов
    if is_vip_code_available(new_code):
        # Если код доступен в таблице VIP, продолжаем как обычно
        if update_personal_code(old_code, new_code):
            if contact_id:
                logging.info('update_contact_code_in_bitrix called')
                update_contact_code_in_bitrix(contact_id, new_code)
            remove_vip_code(new_code)

            # Отправляем уведомление пользователю
            chat_id = get_chat_id_by_personal_code(new_code)
            if chat_id:
                await bot.send_message(
                    chat_id,
                    f"Ваш персональный код был изменен на {new_code}.\n"
                    "Для обновления инструкции по заполнению адреса нажмите на кнопку \"Инструкция по заполнению адреса\" в главном меню бота."
                )
            else:
                logging.warning(f"Chat ID для пользователя с кодом {new_code} не найден.")

            await message.answer(f"Клиенту с кодом {old_code} присвоен новый VIP код {new_code} из таблицы.")
        else:
            await message.answer(f"Ошибка: пользователь с кодом {old_code} не найден или произошла ошибка при обновлении.")
    else:
        # Если код не в базе VIP, проверяем, не используется ли он
        if not is_code_used_by_another_client(new_code):
            if update_personal_code(old_code, new_code):
                if contact_id:
                    logging.info('update_contact_code_in_bitrix called')
                    update_contact_code_in_bitrix(contact_id, new_code)

                # Отправляем уведомление пользователю
                chat_id = get_chat_id_by_personal_code(new_code)
                if chat_id:
                    await bot.send_message(
                        chat_id,
                        f"Ваш персональный код был изменен на {new_code}.\n"
                        "Для обновления инструкции по заполнению адреса нажмите на кнопку \"Инструкция по заполнению адреса\" в главном меню бота."
                    )
                else:
                    logging.warning(f"Chat ID для пользователя с кодом {new_code} не найден.")

                await message.answer(f"Клиенту с кодом {old_code} присвоен новый код {new_code}, отсутствующий в базе VIP.")
            else:
                await message.answer(f"Ошибка: пользователь с кодом {old_code} не найден или произошла ошибка при обновлении.")
        else:
            await message.answer(f"Код {new_code} уже используется другим пользователем.")


@dp.message(Command("delete_track"))
async def delete_track_command(message: Message):
    """
    Команда для удаления сделки и связанной информации.
    Формат команды:
        /delete_track deal {deal_id} - для удаления по ID сделки.
        /delete_track number {track_number} - для удаления по трек-номеру.
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    # Разделяем сообщение для получения параметра
    command_parts = message.text.split()
    if len(command_parts) < 3:
        await message.answer("Укажите тип идентификатора (`deal` или `number`) и его значение.\n"
                             "Пример: `/delete_track deal 12345` или `/delete_track number TEST123`")
        return

    identifier_type = command_parts[1].strip().lower()
    identifier_value = command_parts[2].strip()

    if identifier_type == "deal":
        if not identifier_value.isdigit():
            await message.answer("ID сделки должен быть числом. Пример: `/delete_track deal 12345`")
            return

        deal_id = int(identifier_value)
        deal_info = await get_deal_info(deal_id)
        if deal_info:
            track_number = deal_info.get('UF_CRM_1723542556619', '')
            if track_number:
                await delete_track_and_related_data(track_number, deal_id)
                await message.answer(f"Сделка с ID {deal_id} и трек-номером {track_number} успешно удалена.")
            else:
                await message.answer(f"Не удалось найти трек-номер для сделки с ID {deal_id}.")
        else:
            await message.answer(f"Не удалось найти сделку с ID {deal_id}.")
    elif identifier_type == "number":
        track_number = identifier_value
        deals = get_deals_by_track_ident(track_number)
        if deals:
            for deal in deals:
                deal_id = deal.get('ID')
                await delete_track_and_related_data(track_number, deal_id)
            await message.answer(f"Сделка(и) с трек-номером {track_number} успешно удалена(ы).")
        else:
            # Удаляем трек-номер только из базы данных
            await delete_track_and_related_data(track_number, None)
            await message.answer(f"Сделка с трек-номером {track_number} не найдена в Битрикс. "
                                 f"Трек-номер удален только из базы данных.")
    else:
        await message.answer("Неверный тип идентификатора. Используйте `deal` для ID сделки или `number` для трек-номера.\n"
                             "Пример: `/delete_track deal 12345` или `/delete_track number TEST123`")


async def delete_track_and_related_data(track_number, deal_id):
    """
    Удаляет информацию о сделке из базы данных и Битрикс.
    Если сделка в Битрикс отсутствует, удаляет только трек-номер из базы данных.
    """
    try:
        if deal_id:
            # Проверяем, существует ли сделка в Битрикс
            deal_info = await get_deal_info(deal_id)
            if deal_info:
                # Удаляем сделку из Битрикс
                delete_result = delete_deal(deal_id)
                if delete_result:
                    logging.info(f"Сделка с ID {deal_id} успешно удалена из Битрикс.")
                else:
                    logging.error(f"Ошибка при удалении сделки с ID {deal_id} из Битрикс.")
            else:
                logging.warning(f"Сделка с ID {deal_id} не найдена в Битрикс. Удаляем только из базы данных бота.")
        else:
            logging.warning(f"Сделка с трек-номером {track_number} не найдена в Битрикс. Удаляем только из базы данных.")

        # Удаляем трек-номер из базы данных
        await delete_deal_by_track_number(track_number)
        logging.info(f"Трек-номер {track_number} успешно удален из базы данных.")
    except Exception as e:
        logging.error(f"Ошибка при удалении данных для трек-номера {track_number} и сделки ID {deal_id}: {e}")


@dp.message(Command("delete_client"))
async def delete_client_command(message: Message):
    """
    Команда для удаления клиента из таблицы `clients` по номеру телефона.
    Формат команды:
        /delete_client {phone}
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    # Разделяем сообщение для получения номера телефона
    command_parts = message.text.split()
    if len(command_parts) != 2:
        await message.answer("Укажите номер телефона для удаления.\nПример: `/delete_client 87775554433`")
        return

    phone = command_parts[1].strip()

    # Проверяем, что введён номер телефона
    if not phone:
        await message.answer("Вы не указали номер телефона. Пример: `/delete_client 87775554433`")
        return

    # Вызываем функцию для удаления клиента
    is_deleted = delete_client_from_db(phone)
    if is_deleted:
        await message.answer(f"Запись с номером телефона {phone} успешно удалена.")
    else:
        await message.answer(f"Запись с номером телефона {phone} не найдена.")


@dp.message(Command("export_db"))
async def export_database(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    try:
        output_file = export_database_to_excel()
        document = FSInputFile(output_file)  # Оборачиваем в FSInputFile
        await message.answer_document(document=document, caption="📂 Вот ваша актуальная версия базы данных")
    except Exception as e:
        await message.answer(f"⚠ Ошибка при выгрузке базы данных: {e}")


@dp.message(Command("get_final_deals"))
async def get_final_deals_command(message: Message):
    """
    Команда для получения всех итоговых сделок по contact_id.
    Формат команды: /get_final_deals {contact_id}
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    command_parts = message.text.split()
    if len(command_parts) != 2:
        await message.answer("Укажите contact_id. Пример: /get_final_deals 12345")
        return

    contact_id = command_parts[1].strip()
    deals = get_all_final_deals_by_contact_id(contact_id)
    if deals:
        response = f"Итоговые сделки для contact_id {contact_id}:\n"
        for deal in deals:
            response += (
                f"ID: {deal['id']}, final_deal_id: {deal['final_deal_id']}, "
                f"creation_date: {deal['creation_date']}, stage: {deal['current_stage_id']}, "
                f"track_numbers: {deal['track_numbers']}, вес: {deal['total_weight']}, "
                f"сумма: {deal['total_amount']}, заказы: {deal['number_of_orders']}\n"
            )
        await message.answer(response)
    else:
        await message.answer(f"Итоговые сделки для contact_id {contact_id} не найдены.")


@dp.message(Command("delete_final_deal"))
async def delete_final_deal_command(message: Message):
    """
    Команда для удаления итоговой сделки из таблицы final_deals по final_deal_id.
    Формат команды: /delete_final_deal {final_deal_id}
    """
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    command_parts = message.text.split()
    if len(command_parts) != 2:
        await message.answer("Укажите final_deal_id для удаления. Пример: /delete_final_deal 12345")
        return

    final_deal_id = command_parts[1].strip()
    if delete_final_deal_from_db(final_deal_id):
        await message.answer(f"Итоговая сделка с final_deal_id {final_deal_id} успешно удалена.")
    else:
        await message.answer(f"Итоговая сделка с final_deal_id {final_deal_id} не найдена.")
# ========== Запуск сервисов ==========


async def start_services():
    logging.info("Запуск всех сервисов...")
    await set_bot_commands()
    import uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=3303, log_level="info")
    server = uvicorn.Server(config)
    try:
        await asyncio.gather(server.serve(), webhook_timer(), dp.start_polling(bot))
    except Exception as e:
        logging.error(f"Ошибка в одной из задач: {e}")
    finally:
        logging.info("Сервисы корректно завершены.")


def run_bot_and_server():
    init_db()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_services())


# Запуск приложения
if __name__ == '__main__':
    run_bot_and_server()
