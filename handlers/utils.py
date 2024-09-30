async def delete_previous_message(message, state):
    user_data = await state.get_data()
    last_user_message_id = user_data.get('last_user_message_id')
    last_bot_message_id = user_data.get('last_bot_message_id')

    # Удаляем предыдущее сообщение пользователя
    if last_user_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=last_user_message_id)
        except Exception as e:
            print(f"Ошибка при удалении сообщения пользователя: {e}")

    # Удаляем предыдущее сообщение бота
    if last_bot_message_id:
        try:
            await message.bot.delete_message(chat_id=message.chat.id, message_id=last_bot_message_id)
        except Exception as e:
            print(f"Ошибка при удалении сообщения бота: {e}")


async def send_and_delete_previous(message, text, reply_markup=None, state=None):
    await delete_previous_message(message, state)
    new_message = await message.answer(text, reply_markup=reply_markup)

    # Сохраняем ID последнего сообщения бота
    await state.update_data(last_bot_message_id=new_message.message_id)
    # Сохраняем ID последнего сообщения пользователя
    await state.update_data(last_user_message_id=message.message_id)
