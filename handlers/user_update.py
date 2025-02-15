import logging
from aiogram import Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from db_management import update_client_data, get_client_by_chat_id, get_chat_id_by_phone
from handlers.menu_handling import show_inline_menu
from functions import transliterate, format_phone, validate_phone, generate_address_instructions
from keyboards import create_inline_main_menu, create_city_keyboard, create_pickup_keyboard, create_menu_button
from bitrix_integration import update_contact
from states import Upd
from handlers.utils import send_and_delete_previous


router = Router()


def register_handlers(router_object):
    router_object.message.register(process_update_name, Upd.name)
    router_object.message.register(process_update_phone, Upd.phone)
    router_object.callback_query.register(process_update_city, Upd.city)
    router_object.callback_query.register(process_update_pickup, Upd.pickup_point)


@router.message(Upd.name)
async def process_update_name(message: Message, state: FSMContext):
    if message.text == "📋 Меню":
        await show_inline_menu(message, state)
        return

    await state.update_data(name_cyrillic=message.text.title())
    translit_name = transliterate(message.text)
    await state.update_data(name_translit=translit_name)
    chat_id = message.chat.id
    await state.update_data(chat_id=chat_id)
    await send_and_delete_previous(
        message,
        "Напишите Ваш номер телефона в формате 8xxxxxxxxxx",
        reply_markup=create_menu_button(),
        state=state
    )
    await state.set_state(Upd.phone)
    logging.info("Состояние установлено: phone")


@router.message(Upd.phone)
async def process_update_phone(message: Message, state: FSMContext):
    if message.text == "📋 Меню":
        await show_inline_menu(message, state)
        return

    phone = format_phone(message.text)
    if validate_phone(phone):
        # Проверяем, существует ли пользователь с таким номером телефона
        existing_chat_id = get_chat_id_by_phone(phone)
        if existing_chat_id and existing_chat_id != message.chat.id:
            await send_and_delete_previous(
                message,
                "Пользователь с этим номером телефона уже зарегистрирован в системе. "
                "Если вы хотите обновить данные, используйте команду /start.",
                state=state
            )
            return

        # Если номер телефона уникален или принадлежит текущему пользователю, обновляем данные
        await state.update_data(phone=phone)
        await send_and_delete_previous(
            message,
            "Выберите из какого Вы города",
            reply_markup=create_city_keyboard(),
            state=state
        )
        await state.set_state(Upd.city)
        logging.info("Состояние установлено: city")
    else:
        await send_and_delete_previous(
            message,
            "Пожалуйста, укажите корректный номер телефона в формате 8xxxxxxxxxx",
            state=state
        )


@router.callback_query(Upd.city)
async def process_update_city(callback: CallbackQuery, state: FSMContext):
    if callback.data == "main_menu":
        await show_inline_menu(callback.message, state)
        return

    city = callback.data.split('_')[1]
    await state.update_data(city=city)
    await send_and_delete_previous(
        callback.message,
        "Откуда Вам удобнее забирать товар?",
        reply_markup=create_pickup_keyboard(city),
        state=state
    )
    await state.set_state(Upd.pickup_point)
    logging.info("Состояние установлено: pickup_point")


@router.callback_query(Upd.pickup_point)
async def process_update_pickup(callback: CallbackQuery, state: FSMContext):
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
    old_client_data = get_client_by_chat_id(chat_id)
    personal_code = old_client_data.get('personal_code')
    contact_id = str(old_client_data["contact_id"])

    # Обновление данных клиента
    update_contact(contact_id, name_translit, personal_code, phone, city, pickup_point)
    await state.update_data(contact_id=contact_id)
    update_client_data(
        chat_id=chat_id,
        contact_id=contact_id,
        personal_code=personal_code,
        name_cyrillic=name_cyrillic,
        name_translit=name_translit,
        phone=phone,
        city=city,
        pickup_point=pickup_point
    )

    # Генерация инструкции
    instruction_message = generate_address_instructions(
        name_cyrillic=name_cyrillic,
        personal_code=personal_code,
        name_translit=name_translit,
        pickup_point_code=pickup_point
    )

    # Отправка сообщения
    sent_message = await callback.message.answer(instruction_message, reply_markup=create_inline_main_menu(), parse_mode="MarkdownV2")

    try:
        # Обновление закрепленного сообщения
        chat_info = await callback.message.bot.get_chat(callback.message.chat.id)
        if chat_info.pinned_message:
            await callback.message.bot.unpin_all_chat_messages(chat_id=callback.message.chat.id)
        await callback.message.bot.pin_chat_message(chat_id=callback.message.chat.id, message_id=sent_message.message_id)
        await state.clear()
    except Exception as e:
        logging.error(f"Ошибка при закреплении сообщения: {e}")
