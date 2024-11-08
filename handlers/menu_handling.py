import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.filters.command import Command
from states import Track
from keyboards import create_inline_main_menu, create_menu_button


router = Router()


def register_menu_handlers(router_object):
    router_object.message.register(show_inline_menu, F.text == "📋 Меню")
    router_object.callback_query.register(handle_inline_menu, F.data == "main_menu")
    router_object.message.register(handle_menu_command, Command("menu"))
    router_object.callback_query.register(handle_menu_actions, F.data.in_({"add_track"}))
    router_object.message.register(clear_chat_and_reset_state, Command("clear"))


async def show_inline_menu_action(message_or_callback, state: FSMContext):
    """Показывает меню в зависимости от типа сообщения."""
    await state.clear()  # Сбрасываем все состояния
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer("Выберите опцию из меню:", reply_markup=create_inline_main_menu())
    elif isinstance(message_or_callback, CallbackQuery):
        logging.info(message_or_callback)
        if message_or_callback:
            await message_or_callback.message.answer("Выберите опцию из меню:", reply_markup=create_inline_main_menu())


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
    action = callback.data
    if action == "add_track":
        logging.info(f"Текущее состояние: {await state.get_state()}")

        await callback.message.answer("📄 Введите трек-номер: ", reply_markup=create_menu_button())
        await state.set_state(Track.track_number)


@router.message(Command("clear"))
async def clear_chat_and_reset_state(message: Message, state: FSMContext):
    # Очищаем состояние FSM
    # await state.clear()

    # Очищаем чат
    for i in range(100):
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id - i)
        except:
            continue  # Если не удалось удалить какое-то сообщение, продолжаем
    # await message.answer("Чат очищен, состояние сброшено.")
