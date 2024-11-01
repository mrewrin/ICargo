import logging
from aiogram import Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from db_management import update_client_data, get_client_by_chat_id
from handlers.menu_handling import show_inline_menu
from functions import transliterate, format_phone, validate_phone
from keyboards import create_inline_main_menu, create_city_keyboard, create_pickup_keyboard, create_menu_button
from bitrix_integration import update_contact
from states import Upd


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
    # Дальнейшая обработка, если меню не вызвано
    await state.update_data(name_cyrillic=message.text.title())
    translit_name = transliterate(message.text)
    await state.update_data(name_translit=translit_name)
    chat_id = message.chat.id
    await state.update_data(chat_id=chat_id)
    await message.answer("Напишите Ваш номер телефона в формате +7xxxxxxxxxx",
                         reply_markup=create_menu_button())
    await state.set_state(Upd.phone)
    logging.info("Состояние установлено: phone")


@router.message(Upd.phone)
async def process_update_phone(message: Message, state: FSMContext):
    if message.text == "📋 Меню":
        await show_inline_menu(message, state)
        return
    # Дальнейшая обработка, если меню не вызвано
    phone = message.text
    phone = format_phone(phone)
    if validate_phone(phone):
        await state.update_data(phone=phone)
        await message.answer("Выберите из какого Вы города",
                             reply_markup=create_city_keyboard())
        await state.set_state(Upd.city)
        logging.info("Состояние установлено: city")
    else:
        await message.answer("Пожалуйста, укажите корректный номер телефона в формате +7xxxxxxxxxx")


@router.callback_query(Upd.city)
async def process_update_city(callback: CallbackQuery, state: FSMContext):
    if callback.data == "main_menu":
        await show_inline_menu(callback.message, state)
        return
    # Дальнейшая обработка, если меню не вызвано
    city = callback.data.split('_')[1]
    await state.update_data(city=city)
    new_markup = create_pickup_keyboard(city)
    await callback.message.answer("Откуда Вам удобнее забирать товар?", reply_markup=new_markup)
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
    logging.info(f"Старые данные: {old_client_data}")
    personal_code = old_client_data.get('personal_code')
    logging.info(f"Полученный персональный код: {personal_code}")
    # Вызов функции обновления контакта
    contact_id = str(old_client_data["contact_id"])
    logging.info(f"Contact ID is {contact_id}")
    update_contact(contact_id, name_translit, personal_code, phone, city)
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
    if contact_id:
        final_message = (
            f"🙏 Спасибо, {user_data['name_cyrillic']}!\n\n"
            f"📌 Ваш код: 讠AUG{personal_code}\n\n"
            f"📋 Инструкция по заполнению адреса склада в Китае:\n"
            f"1) 讠AUG{personal_code}\n"
            f"2) 18957788787\n"
            f"3) 浙江省 金华市 义乌市\n"
            f"4) 福田街道 龙岗路一街6号 8787库房\n"
            f"({personal_code}_{user_data['name_translit']}_"
            f"{pickup_point.upper()})\n\n"
            f"❗ 3 пункт нужно вводить вручную, остальное можно скопировать и вставить.\n\n"
            f"👇 Ссылка на группу: тут будет ссылка\n"
        )
        sent_message = await callback.message.answer(final_message, reply_markup=create_inline_main_menu())

        try:
            chat_info = await callback.message.bot.get_chat(callback.message.chat.id)
            if chat_info.pinned_message:
                await callback.message.bot.unpin_all_chat_messages(chat_id=callback.message.chat.id)
            await callback.message.bot.pin_chat_message(chat_id=callback.message.chat.id,
                                                        message_id=sent_message.message_id)
            await state.clear()
        except Exception as e:
            logging.error(f"Ошибка при закреплении сообщения: {e}")
