import requests
import logging
from config import webhook_url


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


def get_deal_info(deal_id):
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


def create_contact(name, personal_code, phone, city):
    """
    Создает контакт с указанными именем, телефоном и городом.
    Возвращает ID созданного контакта.
    """
    url = webhook_url + 'crm.contact.add'
    if city == "astana":
        city = "44"
    elif city == "karaganda":
        city = "46"

    params_contact = {
        'fields': {
            'NAME': name,
            'PHONE': [{'VALUE': phone, 'VALUE_TYPE': 'WORK'}],
            'UF_CRM_1723542816833': city,
            'UF_CRM_1726123664764': personal_code
        }
    }

    response = requests.post(url, json=params_contact)

    if response.status_code == 200:
        contact_id = response.json().get('result')
        return contact_id
    else:
        print(f"Ошибка при создании контакта: {response.status_code}")
        print(f"Ответ сервера: {response.text}")
        return None


def update_contact(contact_id, name=None, phone=None, city=None):
    """
    Обновляет данные контакта в Битрикс по contact_id.
    Можно передать любые комбинации параметров для обновления: имя, телефон, город.
    """
    url = webhook_url + 'crm.contact.update'

    # Получаем текущие данные контакта
    existing_contact_data = requests.get(webhook_url + f'crm.contact.get?id={contact_id}').json()

    # Преобразуем название города в код, если город указан
    if city == "astana":
        city = "44"
    elif city == "karaganda":
        city = "46"

    # Поля для обновления
    fields = {}
    if name:
        fields['NAME'] = name
    if phone:
        phone_id = existing_contact_data['result']['PHONE'][0]['ID']  # Получаем ID текущего телефона
        fields['PHONE'] = [{'ID': phone_id, 'VALUE': phone, 'VALUE_TYPE': 'WORK'}]
    if city:
        fields['UF_CRM_1723542816833'] = city

    # Проверяем, есть ли данные для обновления
    if not fields:
        print("Нет данных для обновления.")
        return None

    params_contact = {
        'id': contact_id,
        'fields': fields
    }

    # Отправляем запрос на обновление данных
    response = requests.post(url, json=params_contact)

    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            print(f"Контакт с ID {contact_id} успешно обновлен.")
        else:
            print(f"Ошибка при обновлении контакта: {response.json().get('error_description')}")
    else:
        print(f"Ошибка при обновлении контакта: {response.status_code}")
        print(f"Ответ сервера: {response.text}")


def update_contact_fields_in_bitrix(contact_id, sum_weight, sum_amount):
    """
    Обновляет пользовательские поля контакта в Bitrix.
    Обнуляет поля weight и amount, и обновляет total_weight и total_amount новыми значениями.
    """
    url = webhook_url + 'crm.contact.update'

    # Поля для обновления
    fields = {
        'UF_CRM_1726207792191': '0',  # weight
        'UF_CRM_1726207809637': '0',  # amount
        'UF_CRM_1726837773968': str(sum_weight),  # total_weight
        'UF_CRM_1726837761251': str(sum_amount)  # total_amount
    }

    # Параметры запроса
    params_contact = {
        'id': contact_id,
        'fields': fields
    }

    # Отправляем запрос на обновление данных контакта
    response = requests.post(url, json=params_contact)

    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            logging.info(f"Поля контакта с ID {contact_id} успешно обновлены.")
        else:
            logging.error(f"Ошибка при обновлении контакта: {response.json().get('error_description')}")
    else:
        logging.error(f"Ошибка при обновлении контакта: {response.status_code}")
        logging.error(f"Ответ сервера: {response.text}")


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


def update_deal_contact(deal_id, contact_id, personal_code, phone, city, pickup_point):
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
                'TITLE': f'{personal_code} {pickup_point} {phone}',  # Обновляем имя и пункт выдачи в названии сделки
                'UF_CRM_1723542922949': f'{pickup_code}',  # Поле с кодом пункта выдачи
                'UF_CRM_1725179625': phone,  # Поле для номера телефона (например)
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


def find_deal_by_track_number(track_number):
    url = webhook_url + 'crm.deal.list'

    params = {
        'filter': {'UF_CRM_1723542556619': track_number},
        'select': ['ID', 'TITLE', 'CONTACT_ID']
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        deals = response.json().get('result', [])
        if deals:
            logging.info(f"Найдены сделки с трек-номером {track_number}: {deals}")
            return deals[0]  # Возвращаем первую найденную сделку
    else:
        logging.error(f"Ошибка при поиске сделки по трек-номеру {track_number}: {response.status_code}")
        return None


def detach_contact_from_deal(deal_id, contact_id):
    """
    Отвязывает контакт от сделки в Битрикс.
    """
    url = webhook_url + 'crm.deal.contact.items.delete'

    params = {
        'ID': deal_id,
        'CONTACT_ID': contact_id
    }

    response = requests.post(url, json=params)

    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            logging.info(f"Контакт с ID {contact_id} успешно отвязан от сделки с ID {deal_id}.")
            return True
        else:
            logging.error(f"Не удалось отвязать контакт с ID {contact_id} от сделки с ID {deal_id}. Ответ сервера: {response.text}")
            return False
    else:
        logging.error(f"Ошибка при отвязывании контакта с ID {contact_id} от сделки с ID {deal_id}: {response.status_code}. Ответ сервера: {response.text}")
        return False


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


def update_standard_deal_fields(deal_id, contact_id=None, title=None, phone=None, city=None):
    """
    Обновляет стандартные поля для существующей сделки в Битрикс.
    """
    try:
        url = webhook_url + 'crm.deal.update'

        fields = {}
        if contact_id:
            fields['CONTACT_ID'] = contact_id
        if title:
            fields['TITLE'] = title
        if phone:
            fields['PHONE'] = phone
        if city:
            fields['CITY'] = city

        params_update = {
            'ID': deal_id,
            'fields': fields
        }

        response = requests.post(url, json=params_update)
        response.raise_for_status()

        result = response.json().get('result')
        if result:
            logging.info(f"Стандартные поля сделки {deal_id} успешно обновлены.")
            return True
        else:
            logging.error(f"Не удалось обновить стандартные поля сделки {deal_id}. Ответ: {response.json()}")
            return False

    except requests.RequestException as e:
        logging.error(f"Ошибка при обновлении стандартных полей сделки {deal_id}: {e}")
        return False


def update_custom_deal_fields(deal_id, telegram_id=None, track_number=None, pickup_point=None):
    """
    Обновляет пользовательские поля для существующей сделки в Битрикс.
    """
    try:
        url = webhook_url + 'crm.deal.update'

        fields = {}
        if track_number:
            fields['UF_CRM_1723542556619'] = track_number  # Поле для трек-номера
        if pickup_point:
            pickup_mapping = {
                "pv_karaganda_1": "52",
                "pv_karaganda_2": "54",
                "pv_astana_1": "48",
                "pv_astana_2": "50"
            }
            fields['UF_CRM_1723542922949'] = pickup_mapping.get(pickup_point)  # Поле для пункта выдачи
        if telegram_id:
            fields['UF_CRM_1725179625'] = telegram_id

        params_update = {
            'id': deal_id,
            'fields': fields
        }

        response = requests.post(url, json=params_update)
        response.raise_for_status()

        result = response.json().get('result')
        if result:
            logging.info(f"Пользовательские поля сделки {deal_id} успешно обновлены.")
            return True
        else:
            logging.error(f"Не удалось обновить пользовательские поля сделки {deal_id}. Ответ: {response.json()}")
            return False

    except requests.RequestException as e:
        logging.error(f"Ошибка при обновлении пользовательских полей сделки {deal_id}: {e}")
        return False


def update_contact_code_in_bitrix(contact_id, new_code):
    url = webhook_url + 'crm.contact.update'
    fields = {
        'UF_CRM_1726123664764': new_code
    }
    params_contact = {
        'id': contact_id,
        'fields': fields
    }

    logging.info(f"Обновление контакта в Битрикс. ID: {contact_id}, Новый код: {new_code}")

    # Отправляем запрос на обновление данных
    response = requests.post(url, json=params_contact)

    if response.status_code == 200:
        result = response.json().get('result')
        if result:
            logging.info(f"Контакт с ID {contact_id} успешно обновлен в Битрикс.")
        else:
            logging.error(f"Ошибка при обновлении контакта в Битрикс: {response.json().get('error_description')}")
    else:
        logging.error(f"Ошибка при обновлении контакта в Битрикс: {response.status_code}")
        logging.error(f"Ответ сервера: {response.text}")
