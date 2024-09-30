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
    user_data = get_client_by_chat_id(chat_id)
    contact_id = user_data.get('contact_id')
    personal_code = user_data.get('personal_code')
    name_translit = user_data.get('name_translit')

    if contact_id:
        latest_deal = get_latest_deal_info(contact_id)
        logging.info(latest_deal)

        if latest_deal:
            pickup_code = latest_deal.get('UF_CRM_1723542922949', 'Пункт выдачи не указан')
            pickup_point = {
                "48": "PV_ASTANA_No1",
                "50": "PV_ASTANA_No2",
                "52": "PV_KARAGANDA_No1",
                "54": "PV_KARAGANDA_No2"
            }.get(pickup_code, "Пункт выдачи не указан")

            formatted_message = (
                f"📌 Ваш код: 讠AUG{personal_code}\n\n"
                f"📋 Инструкция по заполнению адреса склада в Китае:\n"
                f"1) 讠AUG{personal_code}\n"
                f"2) 18957788787\n"
                f"3) 浙江省 金华市 义乌市\n"
                f"4) 福田街道 龙岗路一街6号 8787库房\n"
                f"({personal_code}_{name_translit}_{pickup_point.upper()})\n\n"
                f"❗ 3 пункт нужно вводить вручную, остальное можно скопировать и вставить.\n\n"
                f"👇 Ссылка на группу: тут будет ссылка\n"
            )
            sent_message = await callback.message.answer(formatted_message, reply_markup=create_menu_button())
            try:
                chat_info = await callback.message.bot.get_chat(callback.message.chat.id)
                if chat_info.pinned_message:
                    await callback.message.bot.unpin_all_chat_messages(chat_id=callback.message.chat.id)
                await callback.message.bot.pin_chat_message(chat_id=callback.message.chat.id,
                                                            message_id=sent_message.message_id)
                await state.clear()
            except Exception as e:
                logging.error(f"Ошибка при закреплении сообщения: {e}")

        else:
            await callback.message.answer("Не удалось найти последнюю сделку для этого контакта.")
    else:
        await callback.message.answer("Контакт не найден. Пожалуйста, проверьте номер телефона и попробуйте снова.")

    await state.clear()
