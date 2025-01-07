import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.filters.command import Command
from states import Track
from keyboards import create_inline_main_menu, create_support_keyboard
from handlers.utils import delete_previous_message, send_and_delete_previous

router = Router()


def register_menu_handlers(router_object):
    router_object.message.register(show_inline_menu, F.text == "📋 Меню")
    router_object.callback_query.register(handle_inline_menu, F.data == "main_menu")
    router_object.message.register(handle_menu_command, Command("menu"))
    router_object.callback_query.register(handle_menu_actions, F.data.in_({"add_track"}))
    router_object.message.register(clear_chat_and_reset_state, Command("clear"))
    router_object.message.register(handle_all_text_messages, F.text)


async def show_inline_menu_action(message_or_callback, state: FSMContext):
    """Показывает меню в зависимости от типа сообщения."""

    # Определяем, что передано: Message или CallbackQuery
    if isinstance(message_or_callback, Message):
        await send_and_delete_previous(
            message=message_or_callback,
            text="Выберите опцию из меню:",
            reply_markup=create_inline_main_menu(),
            state=state
        )
    elif isinstance(message_or_callback, CallbackQuery):
        # Удаляем предыдущее сообщение через `send_and_delete_previous`
        await send_and_delete_previous(
            message=message_or_callback.message,
            text="Выберите опцию из меню:",
            reply_markup=create_inline_main_menu(),
            state=state
        )
        # Закрываем CallbackQuery, чтобы Telegram не показывал "часики"
        await message_or_callback.answer()
    await state.clear()  # Сбрасываем все состояния


@router.message(F.text == "📋 Меню")
async def show_inline_menu(message: Message, state: FSMContext):
    logging.info("show_inline_menu called")
    await show_inline_menu_action(message, state)


@router.callback_query(F.data == "main_menu")
async def handle_inline_menu(callback: CallbackQuery, state: FSMContext):
    logging.info("handle_inline_menu called")
    await show_inline_menu_action(callback.message, state)


@router.message(Command("menu"))
async def handle_menu_command(message: Message, state: FSMContext):
    logging.info("handle_menu_command called")
    await show_inline_menu_action(message, state)


@router.callback_query(F.data.in_({"add_track"}))
async def handle_menu_actions(callback: CallbackQuery, state: FSMContext):
    await delete_previous_message(callback.message, state)
    action = callback.data
    if action == "add_track":
        logging.info(f"Текущее состояние: {await state.get_state()}")
        await send_and_delete_previous(callback.message, "📄 Введите трек-номер: ", state=state)
        await state.set_state(Track.track_number)


@router.message(Command("clear"))
async def clear_chat_and_reset_state(message: Message, state: FSMContext):
    # Сбрасываем состояние FSM
    await state.clear()

    # Удаляем все предыдущие сообщения
    await send_and_delete_previous(message, "Чат полностью очищен, состояние сброшено.", state=state)


# Обработчик для всех текстовых сообщений
@router.message(F.text)
async def handle_all_text_messages(message: Message, state: FSMContext):
    # Получаем текущее состояние пользователя
    current_state = await state.get_state()
    if current_state:  # Если пользователь находится в процессе выполнения какой-либо команды
        logging.info(f"Пользователь {message.chat.id} в состоянии {current_state}, сообщение: {message.text}")
        return  # Ничего не делаем, оставляем обработку другим хэндлерам

    # Если пользователь не в состоянии (т.е. никаких действий не ожидается), отправляем стандартный ответ
    await send_and_delete_previous(
        message,
        "Если у вас есть вопрос, обратитесь в службу поддержки.",
        reply_markup=create_support_keyboard(),
        state=state
    )
