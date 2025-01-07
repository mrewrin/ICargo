import logging
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from db_management import get_client_by_chat_id, get_track_numbers_by_chat_id, update_track_number, \
    delete_deal_by_track_number, update_track_number_in_all_tables, get_name_track_by_track_number, \
    get_original_date_by_track
from bitrix_integration import get_deals_by_track, delete_deal, update_tracked_deal_in_bitrix, get_deal_info
from keyboards import create_track_keyboard, create_menu_button
from states import Track
from handlers.utils import send_and_delete_previous


router = Router()


def register_package_search_handlers(router_object):
    # Регистрация обработчиков для поиска посылок
    router_object.callback_query.register(process_phone_search, F.data.in_({"find_package"}))
    router_object.callback_query.register(handle_track_status, lambda callback: callback.data.startswith("backtrack_"))
    router_object.callback_query.register(manage_single_track, lambda callback: callback.data.startswith("manage_single_track_"))
    router_object.callback_query.register(process_track_name_update, F.data.startswith("change_track_name_"))
    router_object.message.register(process_track_name_input, Track.track_name_update)
    router_object.callback_query.register(process_track_number_edit, F.data.startswith("edit_track_"))
    router_object.message.register(process_track_number_input, Track.track_number_update)
    router_object.callback_query.register(process_delete_track, F.data.startswith("delete_track_"))


@router.callback_query(F.data.in_({"find_package"}))
async def process_phone_search(callback: CallbackQuery, state: FSMContext):
    await send_and_delete_previous(callback.message, "Ищем ваши посылки...", state=state)
    chat_id = callback.message.chat.id
    user_data = get_client_by_chat_id(chat_id)

    if user_data:
        track_numbers = get_track_numbers_by_chat_id(chat_id)

        if track_numbers:
            track_number_list = [(track[0], track[1]) for track in track_numbers]
            await send_and_delete_previous(
                callback.message,
                f"Ваши текущие посылки:",
                reply_markup=create_track_keyboard(track_number_list),
                state=state
            )
        else:
            await send_and_delete_previous(
                callback.message,
                "У вас нет добавленных трек-номеров.",
                reply_markup=create_menu_button(),
                state=state
            )
    else:
        await send_and_delete_previous(
            callback.message,
            "Контакт не найден. Пожалуйста, проверьте номер телефона и попробуйте снова.",
            state=state
        )
    await state.clear()


@router.callback_query(lambda callback: callback.data.startswith("backtrack_"))
async def handle_track_status(callback: CallbackQuery, state: FSMContext):
    track_number = callback.data.split("_")[1]
    deals = get_deals_by_track(track_number)

    if deals:
        last_deal = deals[0]
        deal_status = last_deal.get('STAGE_ID', 'Неизвестный статус')
        # Получаем оригинальную дату изменения из локальной таблицы
        deal_history = get_original_date_by_track(track_number)
        if deal_history:
            last_modified, stage_id = deal_history
        else:
            last_modified = last_deal.get('DATE_MODIFY', 'Неизвестная дата')
        status_code_list = {
            "C8:NEW": "Добавлен в базу",
            "C8:PREPARATION": "Отгружен со склада Китая",
            "C8:PREPAYMENT_INVOICE": "Прибыл в Алмату",
            "C4:NEW": "Прибыл в ПВ№1 г.Караганда",
            "C6:NEW": "Прибыл в ПВ Астана ESIL",
            "C2:NEW": "Прибыл в ПВ Астана SARY-ARKA"
        }
        deal_status_text = status_code_list.get(deal_status, "Упакован и ожидает выдачи")
        if last_modified != 'Неизвестная дата':
            last_modified = datetime.fromisoformat(last_modified).strftime("%H:%M %d.%m.%Y")
        name_track = get_name_track_by_track_number(track_number)
        deal_info = await get_deal_info(last_deal['ID'])
        if deal_info.get('UF_CRM_1729539412') == '1':
            track_numbers = deal_info.get('UF_CRM_1729115312')
            ready_track_numbers = [item.strip() for item in track_numbers.split(",") if item.strip()]

            ready_parcels_text = "\n".join(ready_track_numbers)  # Форматируем трек-номера в столбик

            alert_text = (
                f"📦 Информация о посылке:\n"
                f"Готовые к выдаче посылки:\n"
                f"{ready_parcels_text}\n"  # Выводим только трек-номера
                f"Статус: {deal_status_text}\n"
                f"{last_modified}"
            )
        else:
            alert_text = (
                f"📦 Информация о посылке:\n"
                f"Название: {name_track}\n"
                f"Трек номер: {track_number}\n"
                f"Статус: {deal_status_text}\n"
                f"{last_modified}"
            )

        # Кнопка для управления выбранным треком
        keyboard = create_track_keyboard([(track_number, name_track)], update_name=track_number)
        await callback.answer(alert_text, show_alert=True)
        await callback.message.edit_text(text="Управление трек-номером:", reply_markup=keyboard)
    else:
        await callback.answer("📦 Сделки с этим трек-номером не найдены.", show_alert=True)


@router.callback_query(lambda callback: callback.data.startswith("manage_single_track_"))
async def manage_single_track(callback: CallbackQuery, state: FSMContext):
    track_number = callback.data.split("_")[2]

    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(
            text="✏️ Изменить название",
            callback_data=f"change_track_name_{track_number}"
        ),
        width=1
    )
    keyboard.row(
        InlineKeyboardButton(
            text="✏️ Изменить трек-номер",
            callback_data=f"edit_track_{track_number}"
        ),
        width=1
    )
    keyboard.row(
        InlineKeyboardButton(
            text="❌ Удалить трек-номер",
            callback_data=f"delete_track_{track_number}"
        ),
        width=1
    )
    keyboard.row(
        InlineKeyboardButton(
            text="🔙 Назад в меню",
            callback_data="find_package"
        ),
        width=1
    )

    await callback.message.edit_text(
        f"⚙️ Управление трек-номером {track_number}:",
        reply_markup=keyboard.as_markup()
    )


@router.callback_query(F.data.startswith("change_track_name_"))
async def process_track_name_update(callback: CallbackQuery, state: FSMContext):
    track_number = callback.data.split("_", maxsplit=3)[3]
    await state.update_data(track_number=track_number)
    await send_and_delete_previous(callback.message, f"Введите новое название для трек-номера {track_number}:", state=state)
    await state.set_state(Track.track_name_update)


@router.message(Track.track_name_update)
async def process_track_name_input(message: Message, state: FSMContext):
    user_data = await state.get_data()
    track_number = user_data.get('track_number')
    track_name = message.text.strip()
    chat_id = message.chat.id

    update_track_number(track_number, track_name, chat_id)
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(
            text="🔍 Назад к списку трек-номеров",
            callback_data="find_package"
        ),
        width=1
    )
    keyboard.row(
        InlineKeyboardButton(
            text="📋 Меню",
            callback_data="main_menu"
        ),
        width=1
    )

    await send_and_delete_previous(
        message,
        f"📄 Трек-номер {track_number} успешно обновлен с названием '{track_name}'!",
        reply_markup=keyboard.as_markup(),
        state=state
    )
    await state.clear()


@router.callback_query(F.data.startswith("edit_track_"))
async def process_track_number_edit(callback: CallbackQuery, state: FSMContext):
    track_number = callback.data.split("_", maxsplit=2)[2]

    deals = get_deals_by_track(track_number)
    if deals:
        last_deal = deals[0]
        deal_status = last_deal.get('STAGE_ID', None)

        if deal_status == "C8:NEW":
            await state.update_data(track_number=track_number)
            await send_and_delete_previous(
                callback.message,
                f"Введите новый трек-номер для {track_number}:",
                state=state
            )
            await state.set_state(Track.track_number_update)
        else:
            await send_and_delete_previous(
                callback.message,
                "⚠️ Изменение трек-номера недоступно, так как посылка уже отгружена на складе Китая.",
                state=state
            )
    else:
        await send_and_delete_previous(
            callback.message,
            f"⚠️ Сделка с трек-номером {track_number} не найдена в системе.",
            state=state
        )

    await callback.answer()


@router.message(Track.track_number_update)
async def process_track_number_input(message: Message, state: FSMContext):
    user_data = await state.get_data()
    old_track_number = user_data.get('track_number')
    new_track_number = message.text.strip().upper()
    chat_id = message.chat.id

    if not new_track_number.isalnum():
        await send_and_delete_previous(
            message,
            "⚠️ Трек-номер должен содержать только буквы и цифры. Попробуйте снова.",
            state=state
        )
        return

    existing_deal = get_deals_by_track(new_track_number)
    if existing_deal:
        await send_and_delete_previous(
            message,
            f"⚠️ Трек-номер {new_track_number} уже зарегистрирован в системе. Попробуйте другой.",
            state=state
        )
        return

    try:
        update_track_number_in_all_tables(old_track_number, new_track_number, chat_id)
        update_tracked_deal_in_bitrix(old_track_number, new_track_number)
        # Создаем кастомную клавиатуру
        keyboard = InlineKeyboardBuilder()
        keyboard.row(
            InlineKeyboardButton(
                text="🔍 Назад к списку трек-номеров",
                callback_data="find_package"
            ),
            width=1
        )
        keyboard.row(
            InlineKeyboardButton(
                text="📋 Меню",
                callback_data="main_menu"
            ),
            width=1
        )
        await send_and_delete_previous(
            message,
            f"✅ Трек-номер {old_track_number} успешно изменен на {new_track_number}.",
            reply_markup=keyboard.as_markup(),
            state=state
        )
    except Exception as e:
        logging.error(f"Ошибка при обновлении трек-номера: {e}")
        await send_and_delete_previous(
            message,
            "⚠️ Произошла ошибка при обновлении трек-номера. Пожалуйста, попробуйте снова.",
            state=state
        )

    await state.clear()


@router.callback_query(F.data.startswith("delete_track_"))
async def process_delete_track(callback: CallbackQuery):
    track_number = callback.data.split("_", maxsplit=2)[2]

    deals = get_deals_by_track(track_number)
    if deals:
        last_deal = deals[0]
        deal_status = last_deal.get('STAGE_ID')
        deal_id = last_deal.get('ID')

        # Список стадий, на которых разрешено удаление
        allowed_stages = [
            "C6:UC_VEHS4L", "C6:UC_874DXJ", "C6:WON", "C6:LOSE",
            "C2:UC_8EQX6X", "C2:WON", "C2:LOSE", "C8:NEW"
        ]

        # Проверяем, входит ли стадия в список разрешённых
        if deal_status not in allowed_stages:
            await send_and_delete_previous(
                callback.message,
                f"❌ Трек-номер {track_number} не может быть удален, так как сделка находится на недоступной стадии.",
                state=None
            )
            return

        delete_result = delete_deal(deal_id)
        if not delete_result:
            await send_and_delete_previous(
                callback.message,
                "Ошибка при удалении трек-номера. Попробуйте позже.",
                state=None
            )
            return

    await delete_deal_by_track_number(track_number)
    # Создаем кастомную клавиатуру
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(
            text="🔍 Назад к списку трек-номеров",
            callback_data="find_package"
        ),
        width=1
    )
    keyboard.row(
        InlineKeyboardButton(
            text="📋 Меню",
            callback_data="main_menu"
        ),
        width=1
    )

    await send_and_delete_previous(
        callback.message,
        f"✅ Трек-номер {track_number} успешно удален из системы.",
        reply_markup=keyboard.as_markup(),
        state=None
    )
    await callback.answer()
