import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from db_management import get_client_by_chat_id
from bitrix_integration import get_latest_deal_info
from keyboards import create_menu_button


router = Router()


def register_instructions_handlers(router_object):
    router_object.callback_query.register(process_create_instructions, F.data.in_({"address_instructions"}))


@router.callback_query(F.data.in_({"address_instructions"}))
async def process_create_instructions(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    logging.info('process_create_instructions called')
    chat_id = callback.message.chat.id
    logging.info(f'{chat_id}')

    # Извлекаем данные пользователя из базы
    user_data = get_client_by_chat_id(chat_id)
    contact_id = user_data.get('contact_id')
    personal_code = user_data.get('personal_code')
    name_cyrillic = user_data.get('name_cyrillic')
    name_translit = user_data.get('name_translit')
    pickup_point_code = user_data.get('pickup_point')
    logging.info(f'{pickup_point_code}')

    if contact_id:
        # Определяем инструкцию на основе пункта выдачи из базы
        instructions = {
            "pv_astana_1": (
                f"🙏🏻 Спасибо, {name_cyrillic}!\n\n"
                f"📌 Ваш персональный код: AST{personal_code}\n\n"
                f"📖 Инструкция по заполнению адреса склада в Китае:\n"
                f"1) 佳人AST{personal_code}\n"
                f"2) 18346727700\n"
                f"3) 广东省 佛山市 丹灶镇\n"
                f"4) 金沙银沙南路88号 (佳人AST{personal_code}_{name_translit}_ASTANA+ESIL)"
            ),
            "pv_karaganda_1": (
                f"🙏🏻 Спасибо, {name_cyrillic}!\n\n"
                f"📌 Ваш персональный код: KRG{personal_code}\n\n"              
                f"📖 Инструкция по заполнению адреса склада в Китае:\n"
                f"1) 才子KRG{personal_code}\n"
                f"2) 18346727700\n"
                f"3) 广东省 佛山市 南海区\n"
                f"4) 丹灶镇金沙银沙南路88号 (才子KRG{personal_code}_{name_translit}_KRG+CENTR)"
            )
        }

        instruction_message = instructions.get(pickup_point_code, "Пункт выдачи не указан или не поддерживается.")

        # Отправка сообщения
        sent_message = await callback.message.answer(instruction_message, reply_markup=create_menu_button())

        try:
            # Пин сообщения
            chat_info = await callback.message.bot.get_chat(callback.message.chat.id)
            if chat_info.pinned_message:
                await callback.message.bot.unpin_all_chat_messages(chat_id=callback.message.chat.id)
            await callback.message.bot.pin_chat_message(chat_id=callback.message.chat.id,
                                                        message_id=sent_message.message_id)
            await state.clear()
        except Exception as e:
            logging.error(f"Ошибка при закреплении сообщения: {e}")

    else:
        await callback.message.answer("Контакт не найден. Пожалуйста, проверьте номер телефона и попробуйте снова.")

    await state.clear()
