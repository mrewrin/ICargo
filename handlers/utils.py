import logging


async def delete_previous_message(message, state):
    if state:
        user_data = await state.get_data()
        last_user_message_id = user_data.get('last_user_message_id')
        last_bot_message_id = user_data.get('last_bot_message_id')

        # Получаем информацию о закрепленном сообщении
        try:
            chat_info = await message.bot.get_chat(message.chat.id)
            pinned_message_id = chat_info.pinned_message.message_id if chat_info.pinned_message else None
        except Exception as e:
            logging.error(f"Ошибка при получении информации о чате: {e}")
            pinned_message_id = None

        # Удаляем предыдущее сообщение пользователя, если оно не содержит имя или номер телефона
        if last_user_message_id:
            try:
                last_user_message_text = user_data.get('last_user_message_text', '')
                if not (last_user_message_text.isdigit() or last_user_message_text.replace(' ', '').isalpha()):
                    await message.bot.delete_message(chat_id=message.chat.id, message_id=last_user_message_id)
                    logging.info("Удалено предыдущее сообщение пользователя.")
                else:
                    logging.info("Сообщение с именем или номером телефона не удалено.")
            except Exception as e:
                if "message to delete not found" in str(e).lower():
                    logging.info("Сообщение уже удалено, пропускаем.")
                else:
                    logging.error(f"Ошибка при удалении сообщения пользователя: {e}")

        # Удаляем предыдущее сообщение бота, если оно не закреплено
        if last_bot_message_id and last_bot_message_id != pinned_message_id:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=last_bot_message_id)
                logging.info("Удалено предыдущее сообщение бота.")
            except Exception as e:
                if "message to delete not found" in str(e).lower():
                    logging.info("Сообщение уже удалено, пропускаем.")
                else:
                    logging.error(f"Ошибка при удалении сообщения бота: {e}")


async def send_and_delete_previous(message, text, reply_markup=None, state=None):
    if state is None:
        logging.warning("FSM State is None. Удаление предыдущих сообщений пропущено.")
    else:
        await delete_previous_message(message, state)

    try:
        new_message = await message.answer(text, reply_markup=reply_markup)

        # Если state доступен, сохраняем ID последнего сообщения
        if state is not None:
            await state.update_data(
                last_bot_message_id=new_message.message_id,
                last_user_message_id=message.message_id,
                last_user_message_text=message.text  # Сохраняем текст пользователя
            )

    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения: {e}")


def remove_leading_time(date_str: str) -> str:
    if date_str.startswith("00:00 "):
        return date_str[6:]  # пропускаем первые 6 символов ("00:00 ")
    return date_str