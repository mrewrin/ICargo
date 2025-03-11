from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def create_reply_menu_keyboard():
    menu_button = KeyboardButton(text="📋 Меню")
    reply_keyboard = ReplyKeyboardMarkup(
        keyboard=[[menu_button]],
        resize_keyboard=True
    )
    return reply_keyboard


def create_inline_main_menu():
    menu_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Добавить трек-номер", callback_data="add_track")],
        [InlineKeyboardButton(text="🔍 Отслеживание посылок", callback_data="tracking_view")],  # Новый пункт
        [InlineKeyboardButton(text="⚙️ Управление трек-номерами", callback_data="management_view")],  # Новый пункт
        [InlineKeyboardButton(text="💡 Инструкция по заполнению адреса", callback_data="address_instructions")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
        [InlineKeyboardButton(text="📞 Обратиться в поддержку", url="https://t.me/IceCargoProxyBot")]
    ])
    return menu_keyboard


def create_menu_button():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Меню", callback_data="main_menu")]
    ])
    return keyboard


def create_contact_keyboard():
    contact_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить контактные данные", callback_data="edit_contact_info")],
        [InlineKeyboardButton(text="📋 Меню", callback_data="main_menu")]
    ])
    return contact_keyboard


def create_track_added_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Добавить трек-номер", callback_data="add_track")],
        [InlineKeyboardButton(text="🔍 Назад к списку трек-номеров", callback_data="tracking_view")],
        [InlineKeyboardButton(text="📋 Меню", callback_data="main_menu")]
    ])
    return keyboard


def create_settings_keyboard():
    settings_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Мои контактные данные", callback_data="show_contact_info")],
        [InlineKeyboardButton(text="📋 Меню", callback_data="main_menu")]
    ])
    return settings_keyboard


def create_city_keyboard():
    city_keyboard = InlineKeyboardBuilder()
    city_keyboard.add(InlineKeyboardButton(text="Астана", callback_data="city_astana"))
    # city_keyboard.add(InlineKeyboardButton(text="Караганда", callback_data="city_karaganda"))
    return city_keyboard.as_markup()


def create_pickup_keyboard(city):
    pickup_keyboard = InlineKeyboardBuilder()
    if city == 'astana':
        pickup_keyboard.add(InlineKeyboardButton(text="ПВ Астана-Есиль", callback_data="pv_astana_1"))
        pickup_keyboard.add(InlineKeyboardButton(text="ПВ Астана-Алматинский", callback_data="pv_astana_2"))
    # elif city == 'karaganda':
    #     pickup_keyboard.add(InlineKeyboardButton(text="ПВ Караганда №1", callback_data="pv_karaganda_1"))
        # pickup_keyboard.add(InlineKeyboardButton(text="ПВ Караганда №2", callback_data="pv_karaganda_2"))
    pickup_keyboard.adjust(1)
    return pickup_keyboard.as_markup()


# def create_track_keyboard(track_data, update_name=None):
#     """
#     Функция для создания клавиатуры с трек-номерами.
#     track_data - список кортежей вида (track_number, track_name).
#     update_name - трек-номер, для которого нужно добавить кнопки изменения названия, трек-номера или удаления.
#     """
#     track_keyboard = InlineKeyboardBuilder()
#
#     for track_number, track_name in track_data:
#         if track_number:
#             # Используем track_name или track_number, если track_name отсутствует
#             track_name = track_name or track_number
#             track_keyboard.row(InlineKeyboardButton(
#                 text=track_name,
#                 callback_data=f"backtrack_{track_number}"),
#                 width=1)
#     if update_name:
#         # Добавляем кнопку для изменения названия трек-номера
#         track_keyboard.row(
#             InlineKeyboardButton(
#                 text="✏️ Изменить название трек-номера",
#                 callback_data=f"change_track_name_{update_name}"
#             ),
#             width=1
#         )
#         # Добавляем кнопку для изменения трек-номера
#         track_keyboard.row(
#             InlineKeyboardButton(
#                 text="✏️ Изменить трек-номер",
#                 callback_data=f"edit_track_{update_name}"
#             ),
#             width=1
#         )
#         # Добавляем кнопку для удаления трек-номера
#         track_keyboard.row(
#             InlineKeyboardButton(
#                 text="❌ Удалить трек-номер",
#                 callback_data=f"delete_track_{update_name}"
#             ),
#             width=1
#         )
#     track_keyboard.row(InlineKeyboardButton(
#         text="🔍 Назад к списку трек-номеров",
#         callback_data="find_package"),
#         width=1)
#     track_keyboard.row(InlineKeyboardButton(
#         text="📄 Добавить трек-номер",
#         callback_data="add_track"),
#         width=1)
#     track_keyboard.row(InlineKeyboardButton(
#         text="📋 Меню",
#         callback_data="main_menu"),
#         width=1)
#
#     return track_keyboard.as_markup()


def create_tracking_keyboard(track_data):
    """
    Клавиатура для режима отслеживания: просто кнопки с трек-номерами, без управления.
    """
    tracking_keyboard = InlineKeyboardBuilder()

    for track_number, track_name in track_data:
        if track_number:
            track_name = track_name or track_number
            tracking_keyboard.row(
                InlineKeyboardButton(
                    text=track_name,
                    callback_data=f"backtrack_{track_number}"  # Просто показывает `show_alert`
                ),
                width=1
            )

    tracking_keyboard.row(
        InlineKeyboardButton(text="📋 Меню", callback_data="main_menu"),
        width=1
    )

    return tracking_keyboard.as_markup()


def create_management_keyboard(track_data):
    """
    Клавиатура для режима управления: кнопки с трек-номерами, открывающими меню управления.
    """
    management_keyboard = InlineKeyboardBuilder()

    for track_number, track_name in track_data:
        if track_number:
            track_name = track_name or track_number
            management_keyboard.row(
                InlineKeyboardButton(
                    text=track_name,
                    callback_data=f"manage_single_track_{track_number}"  # Открывает меню управления
                ),
                width=1
            )

    management_keyboard.row(
        InlineKeyboardButton(text="📋 Меню", callback_data="main_menu"),
        width=1
    )
    return management_keyboard.as_markup()


def create_single_track_management_keyboard(track_number, track_name):
    """
    Клавиатура управления для одного трек-номера.
    """
    keyboard = InlineKeyboardBuilder()

    keyboard.row(
        InlineKeyboardButton(
            text=f"{track_name}",
            callback_data="none"  # Просто заголовок, без действия
        ),
        width=1
    )

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
            callback_data="management_view"
        ),
        width=1
    )

    return keyboard.as_markup()


async def update_keyboard(callback: CallbackQuery, new_markup: InlineKeyboardMarkup):
    current_markup = callback.message.reply_markup
    if current_markup != new_markup:
        await callback.message.edit_reply_markup(reply_markup=new_markup)


def create_yes_no_keyboard(yes_cb, no_cb):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Да", callback_data=yes_cb),
            InlineKeyboardButton(text="Нет", callback_data=no_cb)
        ]
    ])
    return keyboard


def create_support_keyboard():
    support_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Обратиться в поддержку", url="https://t.me/IceCargoProxyBot")]
    ])
    return support_keyboard
