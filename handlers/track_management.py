import logging
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from bitrix_integration import create_deal, get_deals_by_track, update_deal_contact, create_deal_with_stage, delete_deal
from db_management import get_client_by_chat_id, save_track_number, save_deal_to_db
from keyboards import create_menu_button, create_track_keyboard
from states import Track, Menu
from handlers.utils import send_and_delete_previous


router = Router()


def register_track_management_handlers(router_object):
    router_object.message.register(process_track_number, Track.track_number)
    # router_object.message.register(process_track_unnamed, F.data == "track_no")
    # router_object.message.register(process_track_named, F.data == "track_yes")
    router_object.message.register(process_track_name_input, Track.track_name)


@router.message(Track.track_number)
async def process_track_number(message: Message, state: FSMContext):
    await message.delete()
    track_number = message.text.strip().upper()
    logging.info(f"Получен трек-номер: {track_number}")
    await state.update_data(track_number=track_number)

    if not track_number.isalnum():
        await message.answer("Трек-номер должен содержать только буквы и цифры. \nВведите корректный трек-номер")
        return

    # # Проверка формата трек-номера
    # if len(track_number) != 13:
    #     await send_and_delete_previous(message, "Трек-номер должен состоять из 13 символов. \n"
    #                                             "Введите корректный трек-номер", state=state)
    #     return
    #

    # # Дополнительная проверка, что трек-номер начинается на "AA" и заканчивается на "CN"
    # if not (track_number.startswith("AA") and track_number.endswith("CN")):
    #     await send_and_delete_previous(message,
    #                                    "Неверный формат трек-номера. "
    #                                    "Пожалуйста, начните с 'AA' и закончите на 'CN'.\n"
    #                                    "Введите корректный трек-номер",
    #                                    state=state)
    #     return
    #
    # logging.info("Формат трек-номера корректен")

    deals = get_deals_by_track(track_number)
    logging.info(f"Сделки, найденные по трек-номеру: {deals}")

    if deals:
        last_deal = deals[0]
        deal_contact = last_deal.get('CONTACT_ID')
        pipeline_stage = last_deal.get('STAGE_ID')  # Получаем этап из last_deal
        category_id = int(last_deal.get('CATEGORY_ID'))

        chat_id = message.chat.id
        user_data = get_client_by_chat_id(chat_id)
        user_contact_id = str(user_data.get('contact_id'))  # Преобразуем в строку
        personal_code = user_data.get('personal_code')
        phone = user_data.get('phone')
        city = user_data.get('city')
        pickup_point = user_data.get('pickup_point')
        logging.info(f'Сравниваем контакт из сделки {deal_contact} с контактом из базы {user_contact_id}')
        if deal_contact and deal_contact != user_contact_id:
            await message.answer(
                "Трек-номер, который вы ввели, уже зарегистрирован в системе и привязан к "
                "другому пользователю. Пожалуйста, проверьте данные или введите другой "
                "трек-номер.",
                reply_markup=create_menu_button()
            )
            return
        elif deal_contact == user_contact_id:
            # Если контакт совпадает, создаем новую сделку и удаляем старую
            logging.info(
                f"Контакт совпадает. Создаем новую сделку на этапе {pipeline_stage} и удаляем старую сделку ID {last_deal['ID']}")
            new_deal_id = create_deal_with_stage(
                contact_id=user_contact_id,
                track_number=track_number,
                personal_code=personal_code,
                pickup_point=pickup_point,
                chat_id=chat_id,
                phone=phone,
                pipeline_stage=pipeline_stage,  # Передаем текущий этап
                category_id=category_id  # Категория для новой сделки
            )

            if new_deal_id:
                logging.info(f"Новая сделка создана с ID: {new_deal_id}. Удаляем старую сделку ID {last_deal['ID']}")
                # Сохраняем данные сделки в локальную базу
                save_deal_to_db(
                    deal_id=new_deal_id,
                    contact_id=user_contact_id,
                    personal_code=personal_code,
                    track_number=track_number,
                    pickup_point=pickup_point,
                    phone=phone,
                    chat_id=chat_id
                )
                delete_result = delete_deal(last_deal['ID'])
                if delete_result:
                    logging.info(f"Старая сделка с ID {last_deal['ID']} успешно удалена.")
                    await message.answer(
                        f"📦 Трек-номер {track_number} успешно обновлен! Создана новая сделка.",
                        reply_markup=create_menu_button()
                    )
                else:
                    logging.error(f"Ошибка при удалении старой сделки ID {last_deal['ID']}")
                    await message.answer(
                        "Ошибка при обновлении сделки. Пожалуйста, попробуйте позже."
                    )
            else:
                logging.error("Ошибка при создании новой сделки.")
                await message.answer(
                    "Ошибка при создании новой сделки. Пожалуйста, попробуйте позже."
                )
        elif not deal_contact:
            logging.info(f"Сделка с трек-номером {track_number} без привязанного контакта. Обновляем контакт.")
            update_result = update_deal_contact(last_deal['ID'], user_contact_id, personal_code, chat_id, phone, city,
                                                pickup_point)

            if update_result:
                logging.info(f"Сделка обновлена: контакт {user_contact_id} добавлен к сделке {last_deal['ID']}")
                save_deal_to_db(
                    deal_id=last_deal['ID'],
                    contact_id=user_contact_id,
                    personal_code=personal_code,
                    track_number=track_number,
                    pickup_point=pickup_point,
                    phone=phone,
                    chat_id=chat_id
                )
                await message.answer(
                    f"📦 Трек-номер {track_number} успешно обновлен с вашим контактом!",
                    reply_markup=create_menu_button()
                )
            else:
                logging.error(f"Ошибка при обновлении сделки {last_deal['ID']}")
                await message.answer(
                    "Ошибка при обновлении сделки. Пожалуйста, попробуйте позже."
                )
    else:
        chat_id = message.chat.id
        logging.info(chat_id)
        user_data = get_client_by_chat_id(chat_id)
        logging.info(user_data)
        contact_id = user_data.get('contact_id')
        personal_code = user_data.get('personal_code')
        pickup_point = user_data.get('pickup_point')
        phone = user_data.get('phone')

        if user_data:
            logging.info("Все необходимые данные для создания сделки присутствуют")
            deal_id = create_deal(contact_id, personal_code, track_number, pickup_point, phone, chat_id)

            if deal_id:
                logging.info(f"Сделка успешно создана с ID: {deal_id}")
                # Сохраняем данные сделки в локальную базу
                save_deal_to_db(
                    deal_id=deal_id,
                    contact_id=contact_id,
                    personal_code=personal_code,
                    track_number=track_number,
                    pickup_point=pickup_point,
                    phone=phone,
                    chat_id=chat_id
                )
                await message.answer(
                    f"📄 Трек-номер {track_number} успешно добавлен!"
                )
                await state.set_state(Menu.main_menu)
            else:
                logging.error("Ошибка при создании сделки")
                await message.answer(
                    "Ошибка при добавлении трек-номера. Пожалуйста, попробуйте позже."
                )
        else:
            logging.warning("Недостаточно данных для создания сделки")
            await message.answer(
                "Не удалось найти данные для создания сделки. Пожалуйста, попробуйте снова."
            )

    # Запрашиваем у пользователя название для трек-номера
    await send_and_delete_previous(
        message,
        "Введите название для трек-номера (для облегчения отслеживания посылки):",
        state=state
    )
    await state.set_state(Track.track_name)


# @router.callback_query(F.data == "track_no")
# async def process_track_unnamed(callback: CallbackQuery, state: FSMContext):
#     user_data = await state.get_data()
#     track_number = user_data.get('track_number')
#     track_name = track_number
#     chat_id = callback.message.chat.id
#     logging.info(f"Название для трек-номера не введено, сохранен под названием: {track_name}")
#     # Сохраняем трек-номер и его название в базу данных
#     save_track_number(track_number, track_name, chat_id)
#     await callback.message.answer(f'Трек-номер сохранен с исходным названием {track_name}.\n'
#                                   f'Вы можете изменить название в любое удобное время в разделе меню '
#                                   f'"Отслеживание посылок"', reply_markup=create_menu_button())
#     await state.clear()
#
#
# @router.callback_query(F.data == "track_yes")
# async def process_track_named(callback: CallbackQuery, state: FSMContext):
#     await callback.message.answer("Введите название для трек-номера:")
#     await state.set_state(Track.track_name)


@router.message(Track.track_name)
async def process_track_name_input(message: Message, state: FSMContext):
    user_data = await state.get_data()
    track_number = user_data.get('track_number')
    track_name = message.text.strip()
    chat_id = message.chat.id
    logging.info(f"Получено название для трек-номера: {track_name}")

    save_track_number(track_number, track_name, chat_id)
    await message.answer(f"📄 Трек-номер {track_number} с названием '{track_name}' успешно добавлен!",
                         reply_markup=create_track_keyboard(track_data=[], update_name=track_number))
    await state.clear()
