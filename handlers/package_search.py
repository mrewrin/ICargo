import logging
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from db_management import get_client_by_chat_id, get_track_numbers_by_chat_id, update_track_number
from bitrix_integration import get_deals_by_track
from keyboards import create_track_keyboard, create_menu_button
from states import Track
from handlers.utils import send_and_delete_previous


router = Router()


def register_package_search_handlers(router_object):
    router_object.callback_query.register(process_phone_search, F.data.in_({"find_package"}))
    router_object.callback_query.register(handle_track_status, lambda callback: callback.data.startswith("backtrack_"))
    router_object.callback_query.register(process_track_name_update, F.data.startswith("change_track_name_"))
    router_object.message.register(process_track_name_input, Track.track_name_update)


@router.callback_query(F.data.in_({"find_package"}))
async def process_phone_search(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()  # Удаляем старое сообщение
    chat_id = callback.message.chat.id
    logging.info(f'{chat_id}')

    user_data = get_client_by_chat_id(chat_id)
    phone = user_data.get('phone', None)
    logging.info(f'{phone}')

    if user_data:
        track_numbers = get_track_numbers_by_chat_id(chat_id)

        if track_numbers:
            track_number_list = [(track[0], track[1]) for track in track_numbers]
            await callback.message.answer(f"Ваш номер телефона: {phone}, \nВаши текущие посылки:",
                                          reply_markup=create_track_keyboard(track_number_list))
        else:
            await callback.message.answer("У вас нет добавленных трек-номеров.")
    else:
        await callback.message.answer("Контакт не найден. Пожалуйста, проверьте номер телефона и попробуйте снова.")
    await state.clear()


@router.callback_query(lambda callback: callback.data.startswith("backtrack_"))
async def handle_track_status(callback: CallbackQuery, state: FSMContext):
    track_number = callback.data.split("_")[1]
    logging.info(f"Получен трек-номер: {track_number}")

    track_numbers = get_track_numbers_by_chat_id(callback.message.chat.id)
    track_number_data = next((track for track in track_numbers if track[0] == track_number), None)

    if track_number_data:
        deals = get_deals_by_track(track_number)
        if deals:
            last_deal = deals[0]
            deal_status = last_deal.get('STAGE_ID', 'Неизвестный статус')
            last_modified = last_deal.get('DATE_MODIFY', 'Неизвестная дата')
            status_code_list = {
                "C8:NEW": "Добавлен в базу",
                "C8:PREPARATION": "Отгружен со склада Китая",
                "C8:PREPAYMENT_INVOICE": "Прибыл в Алмату",
                "C4:NEW": "Прибыл в ПВ№1 г.Караганда",
                "C6:NEW": "Прибыл в ПВ№2 г.Караганда",
                "NEW": "Прибыл в ПВ№1 г.Астана",
                "C2:NEW": "Прибыл в ПВ№2 г.Астана"
            }
            deal_status_text = status_code_list.get(deal_status, "Статус неизвестен")

            if last_modified != 'Неизвестная дата':
                last_modified = datetime.fromisoformat(last_modified)
                last_modified = last_modified.strftime("%H:%M %d.%m.%Y")

            track_numbers.remove(track_number_data)

            track_keyboard = create_track_keyboard([(track[0], track[1]) for track in track_numbers],
                                                   update_name=track_number)

            await callback.message.answer(
                f"📦 Статус по трек-номеру {track_number}: \n"
                f"Статус: {deal_status_text}\n"
                f"Последнее обновление: {last_modified}",
                reply_markup=track_keyboard
            )
        else:
            await callback.message.answer(f"📦 Сделки с трек-номером {track_number} не найдены.")
    else:
        await callback.message.answer(f"📦 Трек-номер {track_number} не найден в базе данных.")

    await callback.answer()


@router.callback_query(F.data.startswith("change_track_name_"))
async def process_track_name_update(callback: CallbackQuery, state: FSMContext):
    track_number = callback.data.split("_", maxsplit=3)[3]
    await state.update_data(track_number=track_number)
    logging.info(track_number)
    await send_and_delete_previous(callback.message, f"Введите новое название для трек-номера {track_number}:", state=state)
    await state.set_state(Track.track_name_update)


@router.message(Track.track_name_update)
async def process_track_name_input(message: Message, state: FSMContext):
    user_data = await state.get_data()
    track_number = user_data.get('track_number')
    track_name = message.text.strip()
    logging.info(track_name)
    chat_id = message.chat.id

    logging.info(f"Изменение названия для трек-номера {track_number} на {track_name}")

    update_track_number(track_number, track_name, chat_id)

    await message.answer(f"📄 Трек-номер {track_number} успешно обновлен с названием '{track_name}'!",
                         reply_markup=create_menu_button())
    await state.clear()
