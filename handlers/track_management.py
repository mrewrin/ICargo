import logging
from aiogram import Router
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from bitrix_integration import create_deal, get_deals_by_track, update_deal_contact, create_deal_with_stage, delete_deal
from db_management import get_client_by_chat_id, save_track_number, save_deal_to_db, get_track_from_db, save_deal_history
from keyboards import create_menu_button, create_track_added_keyboard
from states import Track, Menu
from handlers.utils import send_and_delete_previous


router = Router()


def register_track_management_handlers(router_object):
    router_object.message.register(process_track_number, Track.track_number)
    router_object.message.register(process_track_name_input, Track.track_name)


@router.message(Track.track_number)
async def process_track_number(message: Message, state: FSMContext):
    await send_and_delete_previous(message, "Обрабатываем трек-номер...", state=state)
    track_number = message.text.strip().upper()
    logging.info(f"Получен трек-номер: {track_number}")
    await state.update_data(track_number=track_number)

    if not track_number.isalnum():
        await send_and_delete_previous(
            message,
            "Трек-номер должен содержать только буквы и цифры. \nВведите корректный трек-номер.",
            state=state
        )
        return

    # Проверка на существование в локальной базе
    existing_track = get_track_from_db(track_number)  # Реализуйте функцию получения записи
    if existing_track:
        await send_and_delete_previous(
            message,
            f"⚠️ Трек-номер {track_number} уже добавлен. Пожалуйста, введите другой трек-номер.",
            state=state
        )
        return

    deals = get_deals_by_track(track_number)
    logging.info(f"Сделки, найденные по трек-номеру: {deals}")

    if deals:
        last_deal = deals[0]
        deal_contact = last_deal.get('CONTACT_ID')
        pipeline_stage = last_deal.get('STAGE_ID')
        category_id = int(last_deal.get('CATEGORY_ID'))

        chat_id = message.chat.id
        user_data = get_client_by_chat_id(chat_id)
        user_contact_id = str(user_data.get('contact_id'))
        personal_code = user_data.get('personal_code')
        name_translit = user_data.get('name_translit')
        phone = user_data.get('phone')
        city = user_data.get('city')
        pickup_point = user_data.get('pickup_point')

        logging.info(f'Сравниваем контакт из сделки {deal_contact} с контактом из базы {user_contact_id}')
        if deal_contact and deal_contact != user_contact_id:
            await send_and_delete_previous(
                message,
                "Трек-номер, который вы ввели, уже зарегистрирован в системе и привязан к "
                "другому пользователю. Пожалуйста, проверьте данные или введите другой "
                "трек-номер.",
                state=state
            )
            return
        elif deal_contact == user_contact_id:
            logging.info(
                f"Контакт совпадает. Создаем новую сделку на этапе {pipeline_stage} и удаляем старую сделку ID {last_deal['ID']}")
            new_deal_id = create_deal_with_stage(
                contact_id=user_contact_id,
                track_number=track_number,
                personal_code=personal_code,
                name_translit=name_translit,
                pickup_point=pickup_point,
                chat_id=chat_id,
                phone=phone,
                pipeline_stage=pipeline_stage,
                category_id=category_id
            )
            save_deal_history(
                deal_id=new_deal_id,
                track_number=track_number,
                original_date_modify=last_deal.get('DATE_MODIFY'),
                stage_id=last_deal.get('STAGE_ID')
            )

            if new_deal_id:
                logging.info(f"Новая сделка создана с ID: {new_deal_id}. Удаляем старую сделку ID {last_deal['ID']}")
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
                    await send_and_delete_previous(
                        message,
                        f"📦 Трек-номер {track_number} успешно обновлен!",
                        reply_markup=create_menu_button(),
                        state=state
                    )
                else:
                    logging.error(f"Ошибка при удалении старой сделки ID {last_deal['ID']}")
                    await send_and_delete_previous(
                        message,
                        "Ошибка при обновлении сделки. Пожалуйста, попробуйте позже.",
                        state=state
                    )
            else:
                logging.error("Ошибка при создании новой сделки.")
                await send_and_delete_previous(
                    message,
                    "Ошибка при создании новой сделки. Пожалуйста, попробуйте позже.",
                    state=state
                )
        elif not deal_contact:
            logging.info(f"Сделка с трек-номером {track_number} без привязанного контакта. Обновляем контакт.")
            update_result = update_deal_contact(last_deal['ID'], user_contact_id, personal_code, name_translit, chat_id, phone, city,
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
                save_deal_history(
                    deal_id=last_deal['ID'],
                    track_number=track_number,
                    original_date_modify=last_deal.get('DATE_MODIFY'),
                    stage_id=last_deal.get('STAGE_ID')
                )
                await send_and_delete_previous(
                    message,
                    f"📦 Трек-номер {track_number} успешно обновлен с вашим контактом!",
                    reply_markup=create_menu_button(),
                    state=state
                )
            else:
                logging.error(f"Ошибка при обновлении сделки {last_deal['ID']}")
                await send_and_delete_previous(
                    message,
                    "Ошибка при обновлении сделки. Пожалуйста, попробуйте позже.",
                    state=state
                )
    else:
        chat_id = message.chat.id
        user_data = get_client_by_chat_id(chat_id)
        contact_id = user_data.get('contact_id')
        personal_code = user_data.get('personal_code')
        pickup_point = user_data.get('pickup_point')
        phone = user_data.get('phone')

        if user_data:
            deal_id = create_deal(contact_id, personal_code, track_number, pickup_point, phone, chat_id)
            if deal_id:
                save_deal_to_db(
                    deal_id=deal_id,
                    contact_id=contact_id,
                    personal_code=personal_code,
                    track_number=track_number,
                    pickup_point=pickup_point,
                    phone=phone,
                    chat_id=chat_id
                )
                await send_and_delete_previous(
                    message,
                    f"📄 Трек-номер {track_number} успешно добавлен!",
                    state=state
                )
                await state.set_state(Menu.main_menu)
            else:
                await send_and_delete_previous(
                    message,
                    "Ошибка при добавлении трек-номера. Пожалуйста, попробуйте позже.",
                    state=state
                )
        else:
            await send_and_delete_previous(
                message,
                "Не удалось найти данные для создания сделки. Пожалуйста, попробуйте снова.",
                state=state
            )

    await send_and_delete_previous(
        message,
        "Введите название для трек-номера (для облегчения отслеживания посылки):",
        state=state
    )
    await state.set_state(Track.track_name)


@router.message(Track.track_name)
async def process_track_name_input(message: Message, state: FSMContext):
    user_data = await state.get_data()
    track_number = user_data.get('track_number')
    track_name = message.text.strip()
    chat_id = message.chat.id

    save_track_number(track_number, track_name, chat_id)
    await send_and_delete_previous(
        message,
        f"📄 Трек-номер {track_number} с названием '{track_name}' успешно добавлен!",
        reply_markup=create_track_added_keyboard(),
        state=state
    )
    await state.clear()
