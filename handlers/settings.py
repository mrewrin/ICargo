import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from db_management import get_client_by_chat_id
from keyboards import create_settings_keyboard, create_contact_keyboard, create_menu_button
from states import Upd
from handlers.utils import send_and_delete_previous


router = Router()


def register_settings_handlers(router_object):
    router_object.callback_query.register(call_settings, F.data.in_({"settings"}))
    router_object.callback_query.register(show_contact_info, F.data == "show_contact_info")
    router_object.callback_query.register(edit_contact_info, F.data == "edit_contact_info")


@router.callback_query(F.data.in_({"settings"}))
async def call_settings(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("⚙️ Настройки:", reply_markup=create_settings_keyboard())


@router.callback_query(F.data == "show_contact_info")
async def show_contact_info(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    user_data = get_client_by_chat_id(chat_id)
    name_cyrillic = user_data.get("name_cyrillic")
    phone = user_data.get("phone")
    city = user_data.get("city")
    cities = {
        "astana": "Астана",
        "karaganda": "Караганда"
    }
    city = cities.get(city)
    pickup_point = user_data.get("pickup_point")
    pickup_points = {
        "pv_karaganda_1": "Пункт выдачи Караганда 1",
        "pv_karaganda_2": "Пункт выдачи Караганда 2",
        "pv_astana_1": "Пункт выдачи Астана 1",
        "pv_astana_2": "Пункт выдачи Астана 2"
    }
    pickup_point = pickup_points.get(pickup_point)
    contact_info_message = (
        f"Ваши текущие контактные данные:\n"
        f"Имя: {name_cyrillic}\n"
        f"Номер мобильного телефона: {phone}\n"
        f"Город: {city}\n"
        f"Пункт выдачи: {pickup_point}\n"
    )
    await callback.message.answer(contact_info_message, reply_markup=create_contact_keyboard())


@router.callback_query(F.data == "edit_contact_info")
async def edit_contact_info(callback: CallbackQuery, state: FSMContext):
    logging.info(f"Получен запрос на редактирование контактной информации от {callback.message.chat.id}")
    await send_and_delete_previous(callback.message, "✏️ Напишите Ваше имя сюда в чат", reply_markup=create_menu_button(), state=state)
    await state.set_state(Upd.name)
    logging.info(f"Состояние установлено: {await state.get_state()}")
