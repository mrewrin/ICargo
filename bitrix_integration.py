import requests
import logging
import httpx
from datetime import datetime
from config import webhook_url, bitrix
from db_management import find_deal_by_track
from tenacity import retry, stop_after_attempt, wait_fixed


# Определение маппинга стадий для каждой воронки
stage_mapping = {
    'ПВ Астана №1': {
        'arrived': 'C6:NEW',
        'awaiting_pickup': 'C6:UC_VEHS4L',
        'archive': 'C6:LOSE',
        'issued': 'C6:WON'
    },
    'ПВ Астана №2': {
        'arrived': 'C2:NEW',
        'awaiting_pickup': 'C2:UC_8EQX6X',
        'archive': 'C2:LOSE',
        'issued': 'C2:WON'
    },
    'ПВ Караганда №1': {
        'arrived': 'C4:NEW',
        'awaiting_pickup': 'C4:UC_VOLZYJ',
        'archive': 'C4:LOSE',
        'issued': 'C4:WON'
    }
}


# Получение информации о сделках и контактах
def get_deals_by_track(track_number):
    """
    Получает список сделок по значению пользовательского поля UF_CRM_1723542556619.
    Возвращает список сделок с полями ID, STAGE_ID, DATE_MODIFY, UF_CRM_1723542556619 и CONTACT_ID.
    """
    url = webhook_url + 'crm.deal.list'

    params_deal = {
        'filter': {
            'UF_CRM_1723542556619': track_number
        },
        'select': ['*']
    }

    response = requests.post(url, json={'filter': params_deal['filter'], 'select': params_deal['select']})

    if response.status_code == 200:
        deals = response.json().get('result', [])
        logging.info(deals)
        return deals
    else:
        print(f"Ошибка при получении сделок: {response.status_code}")
        print(f"Ответ сервера: {response.text}")
        return []


def get_deals_by_track_ident(track_number):
    """
    Получает список сделок по значению пользовательского поля UF_CRM_1723542556619.
    Возвращает список сделок, у которых трек-номер полностью совпадает с указанным значением.
    """
    url = webhook_url + 'crm.deal.list'

    params_deal = {
        'filter': {
            'UF_CRM_1723542556619': track_number  # Ищем по трек-номеру
        },
        'select': ['ID', 'STAGE_ID', 'DATE_MODIFY', 'UF_CRM_1723542556619', 'CONTACT_ID']  # Выбираем только нужные поля
    }

    response = requests.post(url, json={'filter': params_deal['filter'], 'select': params_deal['select']})

    if response.status_code == 200:
        deals = response.json().get('result', [])
        # Фильтруем результаты на стороне Python для точного совпадения
        filtered_deals = [deal for deal in deals if deal.get('UF_CRM_1723542556619') == track_number]
        logging.info(filtered_deals)
        return filtered_deals
    else:
        logging.error(f"Ошибка при получении сделок: {response.status_code}")
        logging.error(f"Ответ сервера: {response.text}")
        return []


async def get_deal_info(deal_id):
    """
    Получает информацию о сделке по её ID.
    """
    url = webhook_url + 'crm.deal.get'
    params = {
        'id': deal_id
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        deal_info = response.json().get('result', {})
        return deal_info
    else:
        print(f"Ошибка при получении информации о сделке: {response.status_code}")
        print(f"Ответ сервера: {response.text}")
        return None


def get_contact_info(contact_id):
    """
    Получает информацию о контакте по его ID.
    Возвращает словарь с полями ID, NAME, LAST_NAME, PHONE, ADDRESS_CITY.
    """
    url = webhook_url + 'crm.contact.get'

    params_contact = {
        'id': contact_id
    }

    response = requests.get(url, params=params_contact)

    if response.status_code == 200:
        contact = response.json().get('result', {})
        return contact
    else:
        print(f"Ошибка при получении контакта: {response.status_code}")
        return None


def get_contact_id_by_phone(phone):
    """
    Использует API Bitrix24 для поиска CONTACT_ID по номеру телефона.
    """
    url = webhook_url + 'crm.contact.list'
    params = {
        'filter': {'PHONE': phone},
        'select': ['ID']
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        result = response.json().get('result', [])
        if result:
            return result[0]['ID']
    else:
        print(f"Ошибка при получении CONTACT_ID: {response.status_code}")
        print(f"Ответ сервера: {response.text}")

    return None


def get_deals_by_contact_id(contact_id):
    """
    Использует API Bitrix24 для получения списка сделок по CONTACT_ID.
    """
    url = webhook_url + 'crm.deal.list'
    params = {
        'filter': {'CONTACT_ID': contact_id},
        'select': ['ID', 'UF_CRM_1723542556619']  # ID и поле с трек-номером
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        logging.info(response)
        return response.json().get('result', [])
    else:
        print(f"Ошибка при получении списка сделок: {response.status_code}")
        print(f"Ответ сервера: {response.text}")

    return []


def get_latest_deal_info(contact_id):
    """
    Получает последнюю сделку для указанного CONTACT_ID из Bitrix24.
    """
    url = webhook_url + 'crm.deal.list'
    params = {
        'filter': {'CONTACT_ID': contact_id},
        'order': {'DATE_CREATE': 'DESC'},
        'select': ['ID', 'TITLE', 'UF_CRM_1723542922949', 'NAME', 'DATE_CREATE']
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        result = response.json().get('result', [])
        if result:
            logging.info(result)
            return result[0]
    else:
        print(f"Ошибка при получении информации о сделке: {response.status_code}")
        print(f"Ответ сервера: {response.text}")

    return None


def get_active_deals_by_contact(contact_id):
    """
    Возвращает список активных сделок для контакта, находящихся на этапах 'Прибыл в Пункт выдачи'.
    """
    url = f"{webhook_url}/crm.deal.list"
    # Этапы "Прибыл в Пункт выдачи"
    stages = ["C4:NEW", "C6:NEW", "NEW", "C2:NEW"]
    params = {
        'filter': {
            'CONTACT_ID': contact_id,
            'STAGE_ID': stages,  # Фильтруем по этапам прибытия в пункт выдачи
        },
        'select': ['ID', 'TITLE', 'STAGE_ID', 'UF_CRM_1723542556619']  # Добавляем поле трек-номера
    }

    response = requests.post(url, json=params)
    if response.status_code == 200:
        deals = response.json().get('result')
        if deals:
            logging.info(f"Найдено {len(deals)} сделок на этапах 'Прибыл в Пункт выдачи' для контакта {contact_id}.")
            return deals
        else:
            logging.info(f"Сделки на этапах 'Прибыл в Пункт выдачи' для контакта {contact_id} не найдены.")
            return None
    else:
        logging.error(f"Ошибка при получении сделок для контакта {contact_id}: {response.text}")
        return None


def find_deal_by_track_number(track_number, current_deal_id=None):
    url = webhook_url + 'crm.deal.list'

    params = {
        'filter': {'UF_CRM_1723542556619': track_number},
        'select': ['ID', 'TITLE', 'CONTACT_ID', 'STAGE_ID']
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        deals = response.json().get('result', [])
        if deals:
            logging.info(f"Найдены сделки с трек-номером {track_number}: {deals}")

            # Исключаем текущую сделку из найденных
            filtered_deals = [deal for deal in deals if deal['ID'] != current_deal_id]
            if filtered_deals:
                # Возвращаем первую подходящую сделку
                logging.info(f"Возвращаем первую подходящую сделку: {filtered_deals[0]}")
                return filtered_deals[0]
            else:
                logging.info(f"Все найденные сделки совпадают с текущей сделкой ID {current_deal_id}.")
                return None
    else:
        logging.error(f"Ошибка при поиске сделки по трек-номеру {track_number}: {response.status_code}")
        return None


def get_final_deal_for_today(contact_id, pipeline_name):
    today_date = datetime.now().strftime('%Y-%m-%d')
    issued_stage_id = stage_mapping.get(pipeline_name, {}).get('issued',
                                                               'WON')  # Получаем идентификатор этапа "Выдан" для указанной воронки

    url = f"{webhook_url}/crm.deal.list"
    params = {
        'filter': {
            'CONTACT_ID': contact_id,
            'STAGE_ID': issued_stage_id,  # Используем корректный этап "Выдан" для данной воронки
            '>DATE_CREATE': today_date + 'T00:00:00',  # Сравнение по сегодняшней дате
            '<DATE_CREATE': today_date + 'T23:59:59'
        },
        'select': ['ID', 'UF_CRM_1727870320443', 'UF_CRM_1729104281', 'UF_CRM_1729115312']  # Поля итоговой сделки
    }
    response = requests.post(url, json=params)
    if response.status_code == 200:
        deals = response.json().get('result')
        if deals:
            return deals[0]  # Возвращаем первую итоговую сделку на сегодня
        else:
            return None
    else:
        logging.error(f"Ошибка при получении итоговой сделки для контакта {contact_id}: {response.text}")
        return None


async def find_final_deal_for_contact(contact_id, exclude_deal_id=None):
    """
    Ищет итоговую сделку для данного контакта по полю "Итоговая сделка",
    с учетом только стадий "awaiting_pickup".
    """
    # Получаем список стадий 'awaiting_pickup' из stage_mapping
    awaiting_pickup_stages = [details['awaiting_pickup'] for details in stage_mapping.values()]

    url = f"{webhook_url}/crm.deal.list"
    params = {
        'filter': {
            'CONTACT_ID': contact_id,
            'UF_CRM_1729539412': '1',  # Поле для поиска итоговой сделки
            'STAGE_ID': awaiting_pickup_stages  # Фильтр по стадиям 'awaiting_pickup'
        },
        'select': ['*']  # Запрашиваем все поля сделки
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=params)
        if response.status_code == 200:
            deals = response.json().get('result')
            for deal in deals:
                if deal['ID'] != exclude_deal_id:
                    logging.info(f"Найдена итоговая сделка для контакта {contact_id} с ID: {deal['ID']}")
                    return deal  # Возвращаем итоговую сделку
            logging.info(f"Итоговая сделка для контакта {contact_id} не найдена.")
            return None
        else:
            logging.error(f"Ошибка при поиске итоговой сделки для контакта {contact_id}: {response.text}")
            return None


# Создание и обновление контактов
# Маппинг значений pickup_point
locations = {
    'pv_astana_1': "ПВ Астана ESIL",
    'pv_astana_2': "ПВ Астана SARY-ARKA",
    'pv_karaganda_1': "ПВ №1"
}


def create_contact(name, personal_code, phone, city, pickup_point):
    """
    Создает контакт с указанными данными и затем обновляет его пользовательское поле UF_CRM_1737381798322.
    Возвращает ID созданного контакта.
    """
    url_create = webhook_url + 'crm.contact.add'

    city_codes = {
        "astana": "44",
        "karaganda": "46"
    }

    city = city_codes.get(city, city)  # Преобразуем город в код, если он есть в словаре

    params_contact = {
        'fields': {
            'NAME': personal_code,
            'PHONE': [{'VALUE': phone, 'VALUE_TYPE': 'WORK'}],
            'UF_CRM_1723542816833': city,
            'UF_CRM_1726123664764': personal_code,
            'UF_CRM_1730093824027': name
        }
    }

    response = requests.post(url_create, json=params_contact)

    if response.status_code == 200:
        contact_id = response.json().get('result')
        if contact_id:
            update_contact_pickup(contact_id, pickup_point)  # Отдельный запрос на обновление пользовательского поля
        return contact_id
    else:
        print(f"Ошибка при создании контакта: {response.status_code}")
        print(f"Ответ сервера: {response.text}")
        return None


def update_contact(contact_id, name=None, personal_code=None, phone=None, city=None, pickup_point=None):
    """
    Обновляет данные контакта в Битрикс по contact_id, а затем обновляет его пользовательское поле UF_CRM_1737381798322.
    """
    url_update = webhook_url + 'crm.contact.update'

    # Получаем текущие данные контакта
    existing_contact_data = requests.get(webhook_url + f'crm.contact.get?id={contact_id}').json()

    city_codes = {
        "astana": "44",
        "karaganda": "46"
    }

    city = city_codes.get(city, city)  # Преобразуем город в код, если он есть в словаре

    # Поля для обновления
    fields = {}
    if personal_code:
        fields['NAME'] = personal_code
        fields['UF_CRM_1730093824027'] = name
    if phone:
        phone_id = existing_contact_data['result']['PHONE'][0]['ID']  # Получаем ID текущего телефона
        fields['PHONE'] = [{'ID': phone_id, 'VALUE': phone, 'VALUE_TYPE': 'WORK'}]
    if city:
        fields['UF_CRM_1723542816833'] = city

    if not fields:
        print("Нет данных для обновления.")
        return None

    params_contact = {
        'id': contact_id,
        'fields': fields
    }

    # Отправляем запрос на обновление данных
    response = requests.post(url_update, json=params_contact)

    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            print(f"Контакт с ID {contact_id} успешно обновлен.")
            update_contact_pickup(contact_id, pickup_point)  # Обновляем пользовательское поле отдельно
        else:
            print(f"Ошибка при обновлении контакта: {response.json().get('error_description')}")
    else:
        print(f"Ошибка при обновлении контакта: {response.status_code}")
        print(f"Ответ сервера: {response.text}")


def update_contact_pickup(contact_id, pickup_point):
    """
    Отдельно обновляет пользовательское поле UF_CRM_1737381798322 у контакта.
    """
    url_userfield_update = webhook_url + 'crm.contact.userfield.update'

    pickup_location = locations.get(pickup_point, "Неизвестный пункт")  # Получаем строковое значение

    params_userfield = {
        'id': contact_id,
        'fields': {
            'UF_CRM_1737381798322': pickup_location
        }
    }

    response = requests.post(url_userfield_update, json=params_userfield)

    if response.status_code == 200:
        print(f"Пользовательское поле UF_CRM_1737381798322 у контакта {contact_id} успешно обновлено.")
    else:
        print(f"Ошибка при обновлении пользовательского поля: {response.status_code}")
        print(f"Ответ сервера: {response.text}")

async def update_contact_fields_in_bitrix(contact_id, sum_weight, sum_amount, order_count):
    """
    Асинхронно обновляет пользовательские поля контакта в Bitrix, устанавливая вес, сумму и количество заказов.
    """
    url = webhook_url + '/crm.contact.update'

    # Поля для обновления
    fields = {
        'UF_CRM_1726207792191': str(sum_weight),  # Вес заказов (weight)
        'UF_CRM_1726207809637': str(sum_amount),  # Сумма заказов (amount)
        'UF_CRM_1730182877': str(order_count)     # Количество заказов (number_of_orders)
    }

    # Параметры запроса
    params_contact = {
        'id': contact_id,
        'fields': fields
    }

    # Отправляем запрос на обновление данных контакта
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=params_contact)

    # Обработка ответа
    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            logging.info(f"Поля контакта с ID {contact_id} успешно обновлены.")
        else:
            logging.error(f"Ошибка при обновлении контакта: {response.json().get('error_description')}")
    else:
        logging.error(f"Ошибка при обновлении контакта: {response.status_code}")
        logging.error(f"Ответ сервера: {response.text}")


def update_contact_code_in_bitrix(contact_id, new_code):
    """
    Обновляет персональный код в Битриксе как `NAME` и как пользовательское поле.
    """
    url = webhook_url + 'crm.contact.update'
    fields = {
        'UF_CRM_1726123664764': new_code,  # Обновляем пользовательское поле
        'NAME': new_code  # Обновляем имя контакта
    }
    params_contact = {
        'id': contact_id,
        'fields': fields
    }

    logging.info(f"Обновление контакта в Битрикс. ID: {contact_id}, Новый код: {new_code}")

    try:
        # Отправляем запрос на обновление данных
        response = requests.post(url, json=params_contact)

        if response.status_code == 200:
            result = response.json().get('result')
            if result:
                logging.info(f"Контакт с ID {contact_id} успешно обновлен в Битрикс.")
                return True
            else:
                error_desc = response.json().get('error_description', 'Неизвестная ошибка')
                logging.error(f"Ошибка при обновлении контакта в Битрикс: {error_desc}")
        else:
            logging.error(f"Ошибка при обновлении контакта в Битрикс: {response.status_code}")
            logging.error(f"Ответ сервера: {response.text}")
    except requests.RequestException as e:
        logging.error(f"Сетевая ошибка при обновлении контакта в Битрикс: {e}")

    return False


# Создание и обновление сделок
def create_deal(contact_id, personal_code, track_number, pickup_point, phone, chat_id):
    """
    Создает сделку, связывая её с указанным контактом и добавляет выбранный пункт выдачи.
    Возвращает ID созданной сделки.
    """
    try:
        url = webhook_url + 'crm.deal.add'
        pickup_mapping = {
            "pv_karaganda_1": "52",
            "pv_karaganda_2": "54",
            "pv_astana_1": "48",
            "pv_astana_2": "50"
        }
        pickup = pickup_mapping.get(pickup_point)

        params_deal = {
            'fields': {
                'TITLE': f'{personal_code} {pickup_point} {phone}',
                'CATEGORY_ID': '8',
                'STAGE_SEMANTIC_ID': 'P',
                'IS_NEW': 'Y',
                'CONTACT_ID': contact_id,
                'UF_CRM_1723542556619': track_number,
                'UF_CRM_1723542922949': f'{pickup}',
                'UF_CRM_1725179625': chat_id
            }
        }

        response = requests.post(url, json=params_deal)
        response.raise_for_status()  # бросить исключение при ошибке HTTP

        deal_id = response.json().get('result')
        return deal_id
    except requests.RequestException as e:
        logging.error(f"Ошибка при создании сделки: {e}")
        return None


def update_deal_contact(deal_id, contact_id, personal_code, name_translit, chat_id, phone, city, pickup_point):
    """
    Обновляет контакт и дополнительные поля для существующей сделки в Битрикс.
    Возвращает результат обновления (True или False).
    """
    try:
        url = webhook_url + 'crm.deal.update'

        # Карта сопоставления пунктов выдачи
        pickup_mapping = {
            "pv_karaganda_1": "52",
            "pv_karaganda_2": "54",
            "pv_astana_1": "48",
            "pv_astana_2": "50"
        }
        pickup_code = pickup_mapping.get(pickup_point)

        # Параметры обновления сделки
        params_update = {
            'ID': deal_id,
            'fields': {
                'CONTACT_ID': contact_id,
                'TITLE': f'{personal_code} {name_translit} {pickup_point} {phone}',  # Обновляем имя и пункт выдачи в названии сделки
                'UF_CRM_1723542922949': f'{pickup_code}',  # Поле с кодом пункта выдачи
                'UF_CRM_1725179625': chat_id,  # Поле для номера телефона (например)
                'UF_CRM_CITY_FIELD': city  # Пример поля для города (уточните правильный ID)
            }
        }

        # Выполняем запрос на обновление
        response = requests.post(url, json=params_update)
        response.raise_for_status()  # выбрасываем исключение при ошибке HTTP

        result = response.json().get('result')
        if result:
            logging.info(f"Сделка {deal_id} успешно обновлена с новыми данными.")
            return True
        else:
            logging.error(f"Не удалось обновить сделку {deal_id}. Ответ: {response.json()}")
            return False

    except requests.RequestException as e:
        logging.error(f"Ошибка при обновлении сделки {deal_id}: {e}")
        return False


def update_tracked_deal_in_bitrix(old_track_number, new_track_number):
    """
    Обновляет трек-номер в сделке Bitrix.
    """
    deals = get_deals_by_track(old_track_number)  # Получаем сделку по старому трек-номеру

    if not deals:
        logging.warning(f"❌ Сделка с трек-номером {old_track_number} не найдена в Bitrix!")
        return False

    deal = deals[0]  # Берем первую найденную сделку
    deal_id = deal['ID']
    url = webhook_url + 'crm.deal.update'

    params = {
        'id': deal_id,
        'fields': {
            'UF_CRM_1723542556619': new_track_number  # Поле для трек-номера
        }
    }

    logging.info(f"📤 Отправка запроса в Bitrix для сделки {deal_id}: {params}")
    response = requests.post(url, json=params)

    if response.status_code == 200:
        result = response.json()
        logging.info(f"📥 Ответ Bitrix: {result}")

        if result.get('result') == True:
            logging.info(f"✅ Трек-номер сделки ID {deal_id} успешно обновлен на {new_track_number}.")
            return True
        else:
            logging.error(f"🚨 Ошибка при обновлении сделки {deal_id}: {result}")
            return False
    else:
        logging.error(f"🚨 Ошибка HTTP {response.status_code} при обновлении сделки {deal_id}: {response.text}")
        return False


def create_deal_with_stage(contact_id, track_number, personal_code, name_translit, pickup_point, chat_id, phone, pipeline_stage, category_id):
    """
    Создает сделку для данного контакта на определенном этапе и с указанием категории.
    """
    logging.info(f"Создание сделки: контакт {contact_id}, трек-номер {track_number}, этап {pipeline_stage}, категория {category_id}")

    # # Получаем этап сделки из stage_mapping
    # stage_id = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup', 'WON')
    # logging.info(f"Этап для новой сделки: {stage_id}")

    pickup_mapping = {
        "pv_karaganda_1": "52",
        "pv_karaganda_2": "54",
        "pv_astana_1": "48",
        "pv_astana_2": "50"
    }
    pickup_code = pickup_mapping.get(pickup_point)

    # Формируем параметры для создания сделки
    url = f"{webhook_url}/crm.deal.add"
    data = {
        'fields': {
            'TITLE': f'{personal_code} {name_translit} {pickup_point} {phone}',
            'CONTACT_ID': contact_id,
            'STAGE_ID': pipeline_stage,
            'CATEGORY_ID': category_id,
            'UF_CRM_1723542922949': pickup_code,  # Поле для кода пункта выдачи
            'UF_CRM_1723542556619': track_number,  # Поле для трек-номера
            'UF_CRM_1725179625': chat_id,  # Поле для номера телефона
            'OPENED': 'Y'  # Сделка открыта
        }
    }

    try:
        # Отправляем запрос на создание сделки
        response = requests.post(url, json=data)
        response.raise_for_status()

        deal_id = response.json().get('result')
        if deal_id:
            logging.info(f"Сделка успешно создана с ID: {deal_id}")
            return deal_id
        else:
            logging.error(f"Не удалось создать сделку. Ответ сервера: {response.json()}")
            return None
    except requests.RequestException as e:
        logging.error(f"Ошибка при создании сделки: {e}")
        return None


def update_deal_stage(deal_id, stage_id):
    url = f"{webhook_url}/crm.deal.update"
    data = {
        'id': deal_id,
        'fields': {
            'STAGE_ID': stage_id
        }
    }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        logging.info(f"Сделка с ID {deal_id} обновлена на этап {stage_id}.")
        return True
    else:
        logging.error(f"Ошибка при обновлении этапа сделки {deal_id}: {response.text}")
        return False


# def update_standard_deal_fields(deal_id, contact_id=None, title=None, phone=None, city=None):
#     """
#     Обновляет стандартные поля для существующей сделки в Битрикс.
#     """
#     try:
#         url = webhook_url + 'crm.deal.update'
#
#         fields = {}
#         if contact_id:
#             fields['CONTACT_ID'] = contact_id
#         if title:
#             fields['TITLE'] = title
#         if phone:
#             fields['PHONE'] = phone
#         if city:
#             fields['CITY'] = city
#
#         params_update = {
#             'ID': deal_id,
#             'fields': fields
#         }
#
#         response = requests.post(url, json=params_update)
#         response.raise_for_status()
#
#         result = response.json().get('result')
#         if result:
#             logging.info(f"Стандартные поля сделки {deal_id} успешно обновлены.")
#             return True
#         else:
#             logging.error(f"Не удалось обновить стандартные поля сделки {deal_id}. Ответ: {response.json()}")
#             return False
#
#     except requests.RequestException as e:
#         logging.error(f"Ошибка при обновлении стандартных полей сделки {deal_id}: {e}")
#         return False

def update_standard_deal_fields(deal_id, contact_id=None, title=None, phone=None, city=None):
    """
    Добавляет операцию обновления стандартных полей сделки в batch.
    """
    fields = {}
    if contact_id:
        fields['CONTACT_ID'] = contact_id
    if title:
        fields['TITLE'] = title
    if phone:
        fields['PHONE'] = phone
    if city:
        fields['CITY'] = city

    operation_data = {
        'ID': deal_id,
        'fields': fields
    }
    bitrix.add_operation('crm.deal.update', operation_data)
    logging.info(f"Операция для обновления стандартных полей сделки {deal_id} добавлена в batch.")


# def update_custom_deal_fields(deal_id, telegram_id=None, track_number=None, pickup_point=None):
#     """
#     Обновляет пользовательские поля для существующей сделки в Битрикс.
#     """
#     try:
#         url = webhook_url + 'crm.deal.update'
#
#         fields = {}
#         if track_number:
#             fields['UF_CRM_1723542556619'] = track_number  # Поле для трек-номера
#         if pickup_point:
#             pickup_mapping = {
#                 "pv_karaganda_1": "52",
#                 "pv_karaganda_2": "54",
#                 "pv_astana_1": "48",
#                 "pv_astana_2": "50"
#             }
#             fields['UF_CRM_1723542922949'] = pickup_mapping.get(pickup_point)  # Поле для пункта выдачи
#         if telegram_id:
#             fields['UF_CRM_1725179625'] = telegram_id
#
#         params_update = {
#             'id': deal_id,
#             'fields': fields
#         }
#
#         response = requests.post(url, json=params_update)
#         response.raise_for_status()
#
#         result = response.json().get('result')
#         if result:
#             logging.info(f"Пользовательские поля сделки {deal_id} успешно обновлены.")
#             return True
#         else:
#             logging.error(f"Не удалось обновить пользовательские поля сделки {deal_id}. Ответ: {response.json()}")
#             return False
#
#     except requests.RequestException as e:
#         logging.error(f"Ошибка при обновлении пользовательских полей сделки {deal_id}: {e}")
#         return False

def update_custom_deal_fields(deal_id, telegram_id=None, track_number=None, pickup_point=None):
    """
    Добавляет операцию обновления пользовательских полей сделки в batch.
    """
    fields = {}
    if track_number:
        fields['UF_CRM_1723542556619'] = track_number
    if pickup_point:
        pickup_mapping = {
            "pv_karaganda_1": "52",
            "pv_karaganda_2": "54",
            "pv_astana_1": "48",
            "pv_astana_2": "50"
        }
        fields['UF_CRM_1723542922949'] = pickup_mapping.get(pickup_point)
    if telegram_id:
        fields['UF_CRM_1725179625'] = telegram_id

    operation_data = {
        'id': deal_id,
        'fields': fields
    }
    bitrix.add_operation('crm.deal.update', operation_data)
    logging.info(f"Операция для обновления пользовательских полей сделки {deal_id} добавлена в batch.")


async def create_final_deal(contact_id, weight, amount, number_of_orders, track_number, personal_code, pickup_point, phone, pipeline_stage, category_id):
    """
    Создает итоговую сделку для данного контакта и обновляет поля контакта с информацией о весе, сумме и количестве заказов.
    """
    logging.info(f"Определяем этап для pipeline_stage: воронка {category_id}, этап {pipeline_stage}")
    # Получаем начальный этап для итоговой сделки в зависимости от переданной воронки
    stage_id = stage_mapping.get(pipeline_stage, {}).get('awaiting_pickup', 'WON')
    logging.info(f"Этап для итоговой сделки: {stage_id}")

    pickup_mapping = {
        "pv_karaganda_1": "52",
        "pv_karaganda_2": "54",
        "pv_astana_1": "48",
        "pv_astana_2": "50"
    }
    pickup = pickup_mapping.get(pickup_point)

    # Проверка значений и установка значений по умолчанию, если они пусты
    weight = float(weight) if weight else 0.0
    amount = float(amount) if amount else 0.0
    number_of_orders = int(number_of_orders) if number_of_orders else 0

    url = f"{webhook_url}/crm.deal.add"
    data = {
        'fields': {
            'TITLE': f'Итоговая сделка: {personal_code} {pickup_point} {phone}',
            'CONTACT_ID': contact_id,
            'STAGE_ID': stage_id,  # Установка корректного этапа
            'CATEGORY_ID': category_id,
            'UF_CRM_1723542922949': f'{pickup}',
            'UF_CRM_1727870320443': weight,  # Поле Вес заказов
            'OPPORTUNITY': amount,  # Поле Сумма заказов
            'UF_CRM_1730185262': number_of_orders,  # Поле Количество заказов
            'UF_CRM_1729115312': track_number,  # Поле для трек-номеров
            'UF_CRM_1729539412': '1',  # Устанавливаем флаг итоговой сделки
            'OPENED': 'Y',  # Сделка открыта
        }
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=data)

    if response.status_code == 200:
        deal_id = response.json().get('result')
        logging.info(f"Создана итоговая сделка для контакта {contact_id} с ID {deal_id}.")

        # Обновление полей контакта
        await update_contact_fields_in_bitrix(contact_id, sum_weight=weight, sum_amount=amount, order_count=number_of_orders)
        return deal_id
    else:
        logging.error(f"Ошибка при создании итоговой сделки: {response.text}")
        return None


async def update_final_deal(deal_id, track_number):
    logging.info(f"Запуск обновления итоговой сделки {deal_id} с трек-номером {track_number}")

    # Получаем информацию о сделке
    deal_info = await get_deal_info(deal_id)

    if not deal_info:
        logging.error(f"Не удалось получить информацию о сделке {deal_id}")
        return False

    # Получаем текущие трек-номера
    current_track_numbers = deal_info.get('UF_CRM_1729115312', '')
    logging.info(f"Текущие трек-номера для сделки {deal_id}: {current_track_numbers}")

    # Обновляем только трек-номера, объединяя с новыми
    updated_track_numbers = f"{current_track_numbers}, {track_number}".strip(', ') if current_track_numbers else track_number

    # Данные для обновления
    url = f"{webhook_url}/crm.deal.update"
    data = {
        'id': deal_id,
        'fields': {
            'UF_CRM_1729115312': updated_track_numbers  # Обновляем только трек-номера
        }
    }

    # Выполняем асинхронный запрос
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(url, json=data)
        if response.status_code == 200:
            logging.info(f"Сделка {deal_id} успешно обновлена.")
            return True
        else:
            logging.error(f"Ошибка обновления сделки {deal_id}: {response.status_code} - {response.text}")
            return False


# Архивация и удаление сделок
# def detach_contact_from_deal(deal_id, contact_id):
#     """
#     Отвязывает контакт от сделки в Битрикс.
#     """
#     url = webhook_url + 'crm.deal.contact.items.delete'
#
#     params = {
#         'ID': deal_id,
#         'CONTACT_ID': contact_id
#     }
#
#     response = requests.post(url, json=params)
#
#     if response.status_code == 200:
#         result = response.json().get('result')
#         if result:
#             logging.info(f"Контакт с ID {contact_id} успешно отвязан от сделки с ID {deal_id}.")
#             return True
#         else:
#             logging.error(f"Не удалось отвязать контакт с ID {contact_id} от сделки с ID {deal_id}. Ответ сервера: {response.text}")
#             return False
#     else:
#         logging.error(f"Ошибка при отвязывании контакта с ID {contact_id} от сделки с ID {deal_id}: {response.status_code}. Ответ сервера: {response.text}")
#         return False
def detach_contact_from_deal(deal_id, contact_id):
    """
    Добавляет операцию отвязывания контакта от сделки в batch.
    """
    operation_data = {
        'ID': deal_id,
        'CONTACT_ID': contact_id
    }
    bitrix.add_operation('crm.deal.contact.items.delete', operation_data)
    logging.info(f"Операция для отвязывания контакта {contact_id} от сделки {deal_id} добавлена в batch.")


def delete_deal(deal_id):
    url = webhook_url + 'crm.deal.delete'

    params = {
        'id': deal_id  # Важно передавать правильный параметр 'id' для удаления
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            logging.info(f"Сделка с ID {deal_id} успешно удалена.")
            return True
    else:
        logging.error(f"Ошибка при удалении сделки с ID {deal_id}: {response.status_code}")
        return False


# def delete_deal(deal_id):
#     """
#     Добавляет операцию удаления сделки в batch.
#     """
#     operation_data = {
#         'id': deal_id
#     }
#     bitrix.add_operation('crm.deal.delete', operation_data)
#     logging.info(f"Операция для удаления сделки {deal_id} добавлена в batch.")


# async def archive_deal(deal_id, pipeline_stage):
#     """
#     Перемещает сделку в архив (этап, соответствующий 'archive' в маппинге), кроме итоговых сделок.
#     """
#     deal_info = await get_deal_info(deal_id)
#     is_final_deal = deal_info.get('UF_CRM_1729539412')  # Проверка, является ли сделка итоговой
#
#     if is_final_deal == '1':
#         logging.info(f"Сделка {deal_id} является итоговой и не будет перемещена в архив.")
#     else:
#         archive_stage_id = pipeline_stage.get('archive', 'LOSE')  # Используем этап "Архив" из маппинга или 'LOSE' по умолчанию
#         update_deal_stage(deal_id, archive_stage_id)
#         logging.info(f"Сделка с ID {deal_id} перемещена в архив.")


def archive_deal(deal_id, pipeline_stage):
    """
    Добавляет операцию для перемещения сделки в архив в batch.
    """
    archive_stage_id = pipeline_stage.get('archive', 'LOSE')
    operation_data = {
        'ID': deal_id,
        'fields': {'STAGE_ID': archive_stage_id}
    }
    bitrix.add_operation('crm.deal.update', operation_data)
    logging.info(f"Операция для перемещения сделки {deal_id} в архив добавлена в batch.")


# Пакетная обработка данных
async def send_batch_request(batch_requests):
    """
    Отправляет пакетный запрос в Bitrix и возвращает результаты.

    :param batch_requests: Словарь запросов для batch-метода.
    :return: Словарь ответов от Bitrix по каждому запросу.
    """
    url = "https://your-bitrix-domain/rest/batch"  # Укажите ваш URL для batch-запросов
    payload = {"cmd": batch_requests}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()  # Проверка на статус ответа
            response_data = response.json()
            return response_data.get("result", {}).get("result", {})
    except httpx.HTTPStatusError as http_err:
        logging.error(f"HTTP ошибка: {http_err}")
    except Exception as e:
        logging.error(f"Ошибка при отправке batch-запроса: {e}")
    return {}


# Неиспользуемый функционал (возможно переиспользование в дальнейшей разработке)
# def archive_deals_by_contact(contact_id, pipeline_name):
#     # Получаем список сделок в стадии "Прибыл в пункт выдачи"
#     deals = get_active_deals_by_contact(contact_id)
#     track_numbers = []  # Список для трек-номеров
#
#     if deals:
#         for deal in deals:
#             deal_id = deal['ID']
#             track_number = deal.get('UF_CRM_1723542556619')  # Получаем трек-номер
#             if track_number:
#                 track_numbers.append(track_number)
#             else:
#                 logging.info(f"Трек-номер не найден для сделки с ID {deal_id}")
#
#             # Используем `stage_mapping` для определения стадии "Архив"
#             archive_stage_id = stage_mapping.get(pipeline_name, {}).get('archive', 'LOSE')
#             update_deal_stage(deal_id, archive_stage_id)
#             logging.info(f"Сделка с ID {deal_id} перемещена в архив.")
#     else:
#         logging.info(f"Нет активных сделок для контакта с ID {contact_id}.")
#
#     track_numbers_str = ', '.join(track_numbers)  # Преобразуем список трек-номеров в строку
#     logging.info(f"Трек-номера для контакта {contact_id}: {track_numbers_str}")
#     return track_numbers_str  # Возвращаем строку с трек-номерами


# def create_new_deal(contact_id, weight, amount, track_numbers, personal_code, pickup_point, phone, pipeline_name):
#     url = f"{webhook_url}/crm.deal.add"
#     stage_id = stage_mapping.get(pipeline_name, {}).get('issued', 'WON')
# Получаем идентификатор этапа "Выдан" для указанной воронки
#     data = {
#         'fields': {
#             'TITLE': f'{personal_code} {pickup_point} {phone}',
#             'CONTACT_ID': contact_id,
#             'STAGE_ID': stage_id,  # Используем корректный идентификатор этапа "Выдан"
#             'UF_CRM_1727870320443': weight,  # Поле Вес заказов
#             'UF_CRM_1729104281': amount,  # Поле Сумма заказов
#             'UF_CRM_1729115312': track_numbers,  # Поле для трек-номеров
#             'OPENED': 'Y',  # Сделка открыта
#         }
#     }
#     response = requests.post(url, json=data)
#     if response.status_code == 200:
#         logging.info(f"Создана новая сделка для контакта {contact_id} с весом {weight},
#         суммой {amount}, трек номерами {track_numbers}.")
#         return response.json().get('result')
#     else:
#         logging.error(f"Ошибка при создании сделки: {response.text}")
#         return None


# # Основная логика обработки сделки
# def process_deal(deal_id, contact_id, weight, amount, track_number, personal_code, pickup_point, phone):
#     final_deal = get_final_deal_for_today(contact_id)
#     if final_deal:
#         # Обновляем существующую итоговую сделку
#         update_final_deal(final_deal['ID'], weight, amount, track_number)
#     else:
#         # Создаём новую итоговую сделку
#         create_final_deal(contact_id, weight, amount, track_number, personal_code, pickup_point, phone)
#
#     # Перемещаем текущую сделку в архив
#     archive_deal(deal_id)
