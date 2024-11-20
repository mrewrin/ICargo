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
    remove_vip_code, get_contact_id_by_code, save_webhook_to_db, get_latest_webhook_timestamp, get_unprocessed_webhooks
from bitrix_integration import update_contact_code_in_bitrix
from aiogram.filters import Command
from aiogram.types import Message, BotCommand, BotCommandScopeDefault, BotCommandScopeChat


# ========== Инициализация бота и приложения ==========

# bot = Bot(token=bot_token)
dp = Dispatcher(storage=MemoryStorage())
dp.include_routers(user_registration.router, user_update.router, menu_handling.router, track_management.router,
                   package_search.router, information_instructions.router, settings.router)

app = FastAPI()

# ========== Настройки и конфигурация ==========

ADMIN_IDS = [414935403]
CHECK_INTERVAL = 10
IDLE_THRESHOLD = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")


# ========== Установка команд для бота ==========

async def set_bot_commands():
    # Устанавливаем команды для пользователей и администраторов
    user_commands = [
        BotCommand(command="/start", description="Начать диалог"),
        BotCommand(command="/menu", description="Открыть меню"),
        BotCommand(command="/clear", description="Очистить чат")
    ]
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    admin_commands = user_commands + [
        BotCommand(command="/broadcast", description="Админ-рассылка (ввести /broadcast {текст рассылки})"),
        BotCommand(command="/reappropriation", description="Переприсваивание VIP номера пользователю "
                                                           "(ввести /reappropriation {старый_код} {новый_VIP_код})"),
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
@app.post("/webhook")
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
    message_text = message.text[len("/broadcast"):].strip() if message.text.startswith("/broadcast") else ""
    if not message_text:
        await message.answer("Пожалуйста, укажите сообщение для рассылки.")
        return

    for chat_id in get_all_chat_ids():
        try:
            await bot.send_message(chat_id=chat_id, text=message_text)
            logging.info(f"Сообщение отправлено пользователю {chat_id}")
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение пользователю {chat_id}: {e}")
    await message.answer("Рассылка завершена.")


@dp.message(Command("reappropriation"))
async def reappropriation(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return
    args = message.text[len("/reappropriation"):].strip().split() if message.text.startswith("/reappropriation") else []
    if len(args) != 2:
        await message.answer(
            "Пожалуйста, укажите текущий и новый VIP код клиента. \n"
            "Пример: /reappropriation {старый_код} {новый_VIP_код}")
        return

    old_code, new_code = args
    contact_id = get_contact_id_by_code(old_code)
    logging.info(f"Получен contact_id: {contact_id}")

    if not is_vip_code_available(new_code):
        await message.answer(f"VIP код {new_code} недоступен или уже присвоен другому пользователю.")
        return

    if update_personal_code(old_code, new_code):
        if contact_id:
            logging.info('update_contact_code_in_bitrix called')
            update_contact_code_in_bitrix(contact_id, new_code)
        remove_vip_code(new_code)
        await message.answer(f"Клиенту с кодом {old_code} присвоен новый VIP код {new_code}.")
    else:
        await message.answer(f"Ошибка: пользователь с кодом {old_code} не найден или произошла ошибка при обновлении.")


# ========== Запуск сервисов ==========

async def start_services():
    logging.info("Запуск всех сервисов...")
    await set_bot_commands()
    import uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="info")
    server = uvicorn.Server(config)
    await asyncio.gather(server.serve(), webhook_timer(), dp.start_polling(bot))
    logging.info("Сервисы корректно завершены.")


def run_bot_and_server():
    init_db()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_services())


# Запуск приложения
if __name__ == '__main__':
    run_bot_and_server()
