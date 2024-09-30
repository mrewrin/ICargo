import logging
import time
from aiogram import Router
from aiogram.types import CallbackQuery, Message
from aiogram.filters.command import Command
from aiogram.fsm.context import FSMContext
from db_management import save_client_data, check_chat_id_exists, generate_unique_code
from handlers.menu_handling import show_inline_menu
from functions import transliterate, format_phone, validate_phone
from keyboards import create_inline_main_menu, create_city_keyboard, create_pickup_keyboard, create_yes_no_keyboard
from bitrix_integration import create_contact
from states import Reg


router = Router()


def register_handlers(router_object):
    router_object.message.register(send_welcome, Command("start"))
    router_object.message.register(process_name, Reg.name)
    router_object.message.register(process_phone, Reg.phone)
    router_object.callback_query.register(process_city, Reg.city)
    router_object.callback_query.register(process_pickup, Reg.pickup_point)


@router.message(Command("start"))
async def send_welcome(message: Message, state: FSMContext):
    await state.clear()

    chat_id = message.chat.id
    if check_chat_id_exists(chat_id):
        await message.answer(
            "Здравствуйте! \n"
            "Контакт с Вашими данными уже зарегистрирован в системе. \n "
            "Хотите обновить данные?",
            reply_markup=create_yes_no_keyboard("edit_contact_info", "main_menu")
        )
    else:
        # Отправляем первое сообщение
        await message.answer(
            "Здравствуйте! Я бот-помощник ICE Cargo. Я помогу Вам получить личный код."
        )
        time.sleep(2)
        # Отправляем второе сообщение с клавиатурой
        await message.answer(text="Напишите Ваше имя сюда в чат: ")
        await state.set_state(Reg.name)


@router.message(Reg.name)
async def process_name(message: Message, state: FSMContext):
    if message.text.replace(' ', '').isalpha():
        await state.update_data(name_cyrillic=message.text.title())
        translit_name = transliterate(message.text)
        await state.update_data(name_translit=translit_name)
        chat_id = message.chat.id
        await state.update_data(chat_id=chat_id)
        logging.info(f"Пользователь ввел имя: {message.text}, translit: {translit_name}")
        await message.answer("Напишите Ваш номер телефона в формате +7xxxxxxxxxx")
        await state.set_state(Reg.phone)
        logging.info("Состояние установлено: phone")
    else:
        await message.answer("Пожалуйста, введите корректное имя.")


@router.message(Reg.phone)
async def process_phone(message: Message, state: FSMContext):
    logging.info(f"Текущее состояние: {await state.get_state()}")

    if message.text == "📋 Меню":
        await show_inline_menu(message, state)
        return

    phone = message.text
    phone = format_phone(phone)
    if validate_phone(phone):
        await state.update_data(phone=phone)
        await message.answer(
            "Выберите из какого Вы города",
            reply_markup=create_city_keyboard()
        )
        await state.set_state(Reg.city)
        logging.info("Состояние установлено: city")
    else:
        await message.answer("Пожалуйста, укажите корректный номер телефона в формате +7xxxxxxxxxx")


@router.callback_query(Reg.city)
async def process_city(callback: CallbackQuery, state: FSMContext):
    if callback.data == "main_menu":
        await show_inline_menu(callback.message, state)
        return
    # Дальнейшая обработка, если меню не вызвано
    city = callback.data.split('_')[1]
    await state.update_data(city=city)
    new_markup = create_pickup_keyboard(city)
    await callback.message.answer("Откуда Вам удобнее забирать товар?", reply_markup=new_markup)
    await state.set_state(Reg.pickup_point)
    logging.info("Состояние установлено: pickup_point")


@router.callback_query(Reg.pickup_point)
async def process_pickup(callback: CallbackQuery, state: FSMContext):
    if callback.data == "main_menu":
        await show_inline_menu(callback.message, state)
        return
    pickup_point = callback.data
    await state.update_data(pickup_point=pickup_point)
    logging.info("Состояние обновлено: pickup_point")
    chat_id = callback.message.chat.id
    user_data = await state.get_data()
    name_cyrillic = user_data.get('name_cyrillic')
    name_translit = user_data.get('name_translit')
    phone = user_data.get('phone')
    city = user_data.get('city')

    # Генерация уникального кода для клиента
    personal_code = generate_unique_code()

    # Вызов функции создания контакта
    contact_id = create_contact(name_translit, personal_code, phone, city)
    await state.update_data(contact_id=contact_id)

    # Сохранение данных клиента в базу данных с уникальным кодом
    save_client_data(
        chat_id=chat_id,
        contact_id=contact_id,
        personal_code=personal_code,
        name_cyrillic=name_cyrillic,
        name_translit=name_translit,
        phone=phone,
        city=city,
        pickup_point=pickup_point
    )

    if contact_id:
        final_message = (
            f"🙏 Спасибо, {user_data['name_cyrillic']}!\n\n"
            f"📌 Ваш код: 讠AUG{personal_code}\n\n"  # Используем personal_code вместо contact_id
            f"📋 Инструкция по заполнению адреса склада в Китае:\n"
            f"1) 讠AUG{personal_code}\n"  # Используем personal_code вместо contact_id
            f"2) 18957788787\n"
            f"3) 浙江省 金华市 义乌市\n"
            f"4) 福田街道 龙岗路一街6号 8787库房\n"
            f"({personal_code}_{user_data['name_translit']}_"
            f"{pickup_point.upper()})\n\n"
            f"❗ 3 пункт нужно вводить вручную, остальное можно скопировать и вставить.\n\n"
            f"👇 Ссылка на группу: тут будет ссылка\n"
        )
        sent_message = await callback.message.answer(final_message,
                                                     reply_markup=create_inline_main_menu())
        logging.info(sent_message)
        try:
            await callback.message.bot.pin_chat_message(chat_id=callback.message.chat.id,
                                                        message_id=sent_message.message_id)
            # logging.info(callback.message.chat.id, sent_message.message_id)
            await state.clear()
        except Exception as e:
            logging.error(f"Ошибка при закреплении сообщения: {e}")
