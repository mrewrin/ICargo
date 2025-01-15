import sqlite3
import random
import logging
import json
from datetime import datetime


# Инициализация и настройка базы данных
def init_db():
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Создаем таблицу clients, если её нет
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id INTEGER PRIMARY KEY,
        contact_id INTEGER,
        personal_code TEXT UNIQUE,  -- Изменен тип данных на TEXT
        name_cyrillic TEXT,
        name_translit TEXT,
        phone TEXT,
        city TEXT,
        pickup_point TEXT
    )
    """)

    # Создаем индекс для поля personal_code, чтобы гарантировать уникальность
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_personal_code ON clients (personal_code)
    """)

    # Создаем таблицу track_numbers для хранения трек-номеров
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS track_numbers (
        track_id INTEGER PRIMARY KEY AUTOINCREMENT,
        track_number TEXT,
        name_track TEXT,
        chat_id INTEGER,
        FOREIGN KEY (chat_id) REFERENCES clients(chat_id)
    )
    """)

    # Создание таблицы tracked_deals
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tracked_deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,          -- ID сделки в Bitrix
        contact_id INTEGER NOT NULL,       -- ID контакта
        personal_code TEXT,                -- Персональный код клиента
        track_number TEXT NOT NULL UNIQUE, -- Уникальный трек-номер
        pickup_point TEXT,                 -- Пункт выдачи
        phone TEXT,                        -- Телефон клиента
        chat_id INTEGER,                   -- ID чата в Telegram
        created_at TEXT DEFAULT CURRENT_TIMESTAMP -- Дата добавления записи
    )
    """)

    # Создаем таблицу vip_codes для хранения доступных VIP номеров
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vip_codes (
        vip_code TEXT UNIQUE  -- Изменен тип данных на TEXT
    )
    """)

    # Заполняем таблицу VIP номерами (если это первый запуск)
    unique_numbers = [f"{i:04d}" for i in range(1, 10)]  # Уникальные номера от 0001 до 0009
    round_numbers = [f"{i}000" for i in range(1, 10)]  # Круглые номера 1000, 2000, ... 9000
    repeating_numbers = [f"{i}{i}{i}{i}" for i in range(1, 10)]  # Повторяющиеся номера 1111, 2222, ... 9999
    mirror_numbers = ['1001', '1221', '1331', '1441', '1551', '1661', '1771', '1881', '1991', '2002', '2112', '2332', '2442', '2552', '2662', '2772', '2882', '2992', '3003', '3113', '3223', '3443', '3553', '3663', '3773', '3883', '3993', '4004', '4114', '4224', '4334', '4554', '4664', '4774', '4884', '4994', '5005', '5115', '5225', '5335', '5445', '5665', '5775', '5885', '5995', '6006', '6116', '6226', '6336', '6446', '6556', '6776', '6886', '6996', '7007', '7117', '7227', '7337', '7447', '7557', '7667', '7887', '7997', '8008', '8118', '8228', '8338', '8448', '8558', '8668', '8778', '8998', '9009', '9119', '9229', '9339', '9449', '9559', '9669', '9779', '9889']
    sequential_numbers = ["1234", "2345", "3456", "4567", "5678", "6789"]

    all_vip_numbers = set(unique_numbers + round_numbers + repeating_numbers + mirror_numbers + sequential_numbers)

    cursor.executemany("INSERT OR IGNORE INTO vip_codes (vip_code) VALUES (?)", [(code,) for code in all_vip_numbers])

    # Создаем таблицу для хранения необработанных вебхуков
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS webhooks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id TEXT,  -- ID сделки или контакта
        event_type TEXT,  -- Тип события
        timestamp TEXT,  -- Время получения вебхука
        processed INTEGER DEFAULT 0  -- Флаг обработки (0 - не обработан, 1 - обработан)
    )
    """)

    # Создаем индексы для таблицы webhooks
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_webhook_entity_id ON webhooks (entity_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_webhook_processed ON webhooks (processed)")

    # Создаем таблицу для хранения обработанных результатов вебхуков
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processed_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id INTEGER,
        event_type TEXT,
        data TEXT,                 -- JSON-строка для хранения дополнительных данных по обработке
        action TEXT,               -- Тип действия: "update", "relink_contact", "archive", "notify" и т.д.
        timestamp TEXT,            -- Время обработки
        sent INTEGER DEFAULT 0     -- Флаг отправки (0 - не отправлено, 1 - отправлено)
    )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS final_deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL,       -- ID контакта, связанного с итоговой сделкой
            final_deal_id INTEGER NOT NULL,    -- ID итоговой сделки в Bitrix
            creation_date TEXT,                -- Дата создания итоговой сделки
            current_stage_id TEXT,             -- Текущий этап сделки
            track_numbers TEXT,                -- Список трек-номеров, связанных с итоговой сделкой
            total_weight REAL DEFAULT 0,       -- Общий вес заказов
            total_amount REAL DEFAULT 0,       -- Общая сумма оплаты
            number_of_orders INTEGER DEFAULT 0 -- Общее количество заказов
        )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deal_tasks (
            deal_id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Таблица для сохранения информации о рассылке
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS broadcast_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        message_id INTEGER NOT NULL,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deal_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        deal_id INTEGER NOT NULL,          -- ID сделки в Bitrix
        track_number TEXT NOT NULL UNIQUE, -- Трек-номер
        original_date_modify TEXT,         -- Оригинальная дата изменения
        stage_id TEXT,                     -- Этап сделки
        created_at TEXT DEFAULT CURRENT_TIMESTAMP -- Время сохранения записи
    )
    """)

    conn.commit()
    conn.close()


# Генерация и проверка уникальных кодов
def generate_unique_code():
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    while True:
        # Генерация случайного 4-значного кода с ведущими нулями
        personal_code = f"{random.randint(1, 9999):04d}"

        # Проверяем, существует ли этот код в базе данных клиентов
        cursor.execute('SELECT personal_code FROM clients WHERE personal_code = ?', (personal_code,))
        result_clients = cursor.fetchone()

        # Проверяем, существует ли этот код в списке VIP-номеров
        cursor.execute('SELECT vip_code FROM vip_codes WHERE vip_code = ?', (personal_code,))
        result_vip = cursor.fetchone()

        # Если код не найден ни в базе клиентов, ни в списке VIP, то он уникален и мы можем выйти из цикла
        if not result_clients and not result_vip:
            break

    conn.close()
    return personal_code


def is_vip_code_available(code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("SELECT vip_code FROM vip_codes WHERE vip_code = ?", (code,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def is_code_used_by_another_client(new_code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 1 FROM clients WHERE personal_code = ?
    """, (new_code,))
    result = cursor.fetchone()
    conn.close()

    return result is not None


def update_personal_code(old_code, new_code):
    """
    Обновляет персональный код в таблицах `clients` и `tracked_deals`.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    try:
        # Обновляем персональный код в таблице `clients`
        cursor.execute("UPDATE clients SET personal_code = ? WHERE personal_code = ?", (new_code, old_code))
        updated_clients = cursor.rowcount > 0

        # Обновляем персональный код в таблице `tracked_deals`
        cursor.execute("UPDATE tracked_deals SET personal_code = ? WHERE personal_code = ?", (new_code, old_code))
        updated_tracked_deals = cursor.rowcount > 0

        # Проверяем, были ли изменения в обеих таблицах
        if updated_clients or updated_tracked_deals:
            conn.commit()
            logging.info(f"Персональный код обновлен с {old_code} на {new_code} в таблицах.")
            return True
        else:
            logging.warning(f"Персональный код {old_code} не найден в таблицах для обновления.")
            return False
    except sqlite3.Error as e:
        logging.error(f"Ошибка при обновлении персонального кода: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def remove_vip_code(code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM vip_codes WHERE vip_code = ?", (code,))
    conn.commit()
    conn.close()


def get_name_track_by_track_number(track_number):
    """
    Получает name_track по track_number из таблицы track_numbers.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name_track FROM track_numbers WHERE track_number = ?", (track_number,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"Ошибка при извлечении name_track для трек-номера {track_number}: {e}")
        return None
    finally:
        conn.close()


def update_name_track_by_track_number(track_number, new_name):
    """
    Обновляет name_track для track_number в таблице track_numbers.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE track_numbers SET name_track = ? WHERE track_number = ?",
            (new_name, track_number)
        )
        conn.commit()  # Фиксируем изменения в базе
        logging.info(f"Название для трек-номера {track_number} успешно обновлено на '{new_name}'.")
    except Exception as e:
        logging.error(f"Ошибка при обновлении name_track для трек-номера {track_number}: {e}")
    finally:
        conn.close()


# Операции с данными клиентов
def save_client_data(chat_id, contact_id, personal_code, name_cyrillic, name_translit, phone, city, pickup_point):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Вставка данных в таблицу
    cursor.execute('''
        INSERT INTO clients 
        (chat_id, contact_id, personal_code, name_cyrillic, name_translit, phone, city, pickup_point)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (chat_id, contact_id, personal_code, name_cyrillic, name_translit, phone, city, pickup_point))

    # Сохраняем изменения и закрываем соединение
    conn.commit()
    conn.close()


def update_client_data(chat_id, contact_id, personal_code, name_cyrillic, name_translit, phone, city, pickup_point):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Обновляем данные в таблице, если chat_id уже существует
    cursor.execute('''
        UPDATE clients 
        SET contact_id = ?, personal_code = ?, name_cyrillic = ?, name_translit = ?, phone = ?, city = ?, pickup_point = ?
        WHERE chat_id = ?
    ''', (contact_id, personal_code, name_cyrillic, name_translit, phone, city, pickup_point, chat_id))

    # Сохраняем изменения и закрываем соединение
    conn.commit()
    conn.close()


def get_all_clients():
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выполняем запрос на выборку всех данных из таблицы
    cursor.execute('SELECT * FROM clients')
    rows = cursor.fetchall()

    # Закрываем соединение
    conn.close()

    return rows


def get_client_by_chat_id(chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute('SELECT contact_id, personal_code, name_cyrillic, name_translit, '
                   'phone, city, pickup_point FROM clients WHERE chat_id = ?',
                   (chat_id,))
    result = cursor.fetchone()
    conn.close()

    if result:
        # Возвращаем данные в формате словаря
        return {
            "contact_id": result[0],
            "personal_code": result[1],
            "name_cyrillic": result[2],
            "name_translit": result[3],
            "phone": result[4],
            "city": result[5],
            "pickup_point": result[6],
            "chat_id": chat_id
        }
    else:
        return None  # Если пользователь не найден, возвращаем None


def get_client_by_contact_id(contact_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM clients WHERE contact_id = ?", (contact_id,))
    result = cursor.fetchone()
    if result:
        return {
            "chat_id": result[0],
            "contact_id": result[1],
            "personal_code": result[2],
            "name_cyrillic": result[3],
            "name_translit": result[4],
            "phone": result[5],
            "city": result[6],
            "pickup_point": result[7]
        }
    return None


def get_chat_id_by_phone(phone):
    """
    Проверяет, зарегистрирован ли пользователь с данным номером телефона.
    Возвращает chat_id, если пользователь найден.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("SELECT chat_id FROM clients WHERE phone = ?", (phone,))
    result = cursor.fetchone()
    conn.close()

    return result[0] if result else None


def check_chat_id_exists(chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выполняем запрос на проверку наличия chat_id
    cursor.execute('SELECT 1 FROM clients WHERE chat_id = ?', (chat_id,))
    result = cursor.fetchone()

    conn.close()

    # Возвращаем True, если запись найдена, иначе False
    return result is not None


def get_all_chat_ids():
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Запрос на получение всех chat_id из таблицы clients
    cursor.execute('SELECT chat_id FROM clients')
    rows = cursor.fetchall()

    conn.close()

    # Преобразуем список кортежей в простой список chat_id
    chat_ids = [row[0] for row in rows]
    return chat_ids


def get_personal_code_by_chat_id(chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute('SELECT personal_code FROM clients WHERE chat_id = ?', (chat_id,))
    result = cursor.fetchone()
    conn.close()

    if result:
        return result[0]  # Возвращаем personal_code
    else:
        return None  # Если personal_code не найден


def get_chat_id_by_personal_code(personal_code):
    """
    Получает chat_id по указанному personal_code из таблицы clients.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute('SELECT chat_id FROM clients WHERE personal_code = ?', (personal_code,))
    result = cursor.fetchone()
    conn.close()

    if result:
        return result[0]  # Возвращаем chat_id
    else:
        return None  # Если chat_id не найден


def get_contact_id_by_code(code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    logging.info(f"Проверка кода в базе данных: {code} (тип: {type(code)})")
    cursor.execute('SELECT contact_id FROM clients WHERE personal_code = ?', (code,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0]
    return None


def get_chat_id_by_contact_id(contact_id):
    """
    Получает chat_id из базы данных по contact_id.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выполняем запрос для получения chat_id по contact_id
    cursor.execute('SELECT chat_id FROM clients WHERE contact_id = ?', (contact_id,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return result[0]  # Возвращаем chat_id
    return None


# Работа с трек-номерами
def save_track_number(track_number, name_track, chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Вставка трек-номера и его названия в таблицу
    cursor.execute('''
        INSERT INTO track_numbers (track_number, name_track, chat_id)
        VALUES (?, ?, ?)
    ''', (track_number, name_track, chat_id))

    conn.commit()
    conn.close()


def update_track_number(track_number, name_track, chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    logging.info(f"Изменение названия для трек-номера {track_number} на {name_track} для пользователя {chat_id}")
    # Обновление названия трек-номера по track_number и chat_id
    cursor.execute('''
        UPDATE track_numbers
        SET name_track = ?
        WHERE track_number = ? AND chat_id = ?
    ''', (name_track, track_number, chat_id))

    conn.commit()
    conn.close()


def update_track_number_in_all_tables(old_track_number, new_track_number, chat_id):
    """
    Обновляет трек-номер в таблицах track_numbers и tracked_deals.
    :param old_track_number: Старый трек-номер, который нужно заменить.
    :param new_track_number: Новый трек-номер, на который нужно заменить.
    :param chat_id: ID чата пользователя.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    logging.info(f"Обновление трек-номера {old_track_number} на {new_track_number} для пользователя {chat_id}")

    try:
        # Обновляем трек-номер в таблице track_numbers
        cursor.execute('''
            UPDATE track_numbers
            SET track_number = ?
            WHERE track_number = ? AND chat_id = ?
        ''', (new_track_number, old_track_number, chat_id))

        # Обновляем трек-номер в таблице tracked_deals
        cursor.execute('''
            UPDATE tracked_deals
            SET track_number = ?
            WHERE track_number = ? AND chat_id = ?
        ''', (new_track_number, old_track_number, chat_id))

        conn.commit()
        logging.info(f"Трек-номер {old_track_number} успешно обновлен на {new_track_number} в таблицах track_numbers и tracked_deals.")
        return True
    except Exception as e:
        logging.error(f"Ошибка при обновлении трек-номера {old_track_number} на {new_track_number}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def get_track_data_by_track_number(track_number):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Поиск трек-номера в таблице track_numbers
    cursor.execute('SELECT track_number, name_track, chat_id FROM track_numbers WHERE track_number = ?',
                   (track_number,))
    result = cursor.fetchone()
    conn.close()

    if result:
        logging.info(result)

        # Возвращаем данные по трек-номеру
        return {
            "track_number": result[0],
            "name_track": result[1],
            "chat_id": result[2]
        }
    else:
        return None  # Если трек-номер не найден


def get_track_numbers_by_chat_id(chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выбор всех трек-номеров для данного chat_id
    cursor.execute('SELECT track_number, name_track FROM track_numbers WHERE chat_id = ?', (chat_id,))
    rows = cursor.fetchall()
    logging.info(rows)
    conn.close()

    return rows


def get_track_from_db(track_number):
    """
    Проверяет наличие трек-номера в базе данных.
    Возвращает True, если трек-номер существует, иначе False.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Поиск трек-номера в таблице track_numbers
    cursor.execute('SELECT 1 FROM track_numbers WHERE track_number = ?', (track_number,))
    result = cursor.fetchone()
    conn.close()

    if result:
        logging.info(f"Трек-номер {track_number} найден в базе данных.")
        return True  # Трек-номер существует
    else:
        logging.info(f"Трек-номер {track_number} не найден в базе данных.")
        return False  # Трек-номер отсутствует


def get_all_track_numbers():
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выбираем все данные из таблицы track_numbers
    cursor.execute('SELECT * FROM track_numbers')
    rows = cursor.fetchall()

    if rows:
        for row in rows:
            print(row)
    else:
        print("Таблица track_numbers пуста.")

    conn.close()


def save_deal_to_db(deal_id, contact_id, personal_code, track_number, pickup_point, phone, chat_id):
    """
    Сохраняет информацию о сделке в таблицу tracked_deals.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO tracked_deals (deal_id, contact_id, personal_code, track_number, pickup_point, phone, chat_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (deal_id, contact_id, personal_code, track_number, pickup_point, phone, chat_id))

        conn.commit()
        logging.info(f"Сделка ID {deal_id} с трек-номером {track_number} успешно сохранена в базе данных.")
    except sqlite3.IntegrityError as e:
        logging.warning(f"Сделка с трек-номером {track_number} уже существует в базе данных: {e}")
    finally:
        conn.close()


def update_tracked_deal(deal_id, track_number):
    """
    Обновляет поле deal_id в таблице tracked_deals для заданного трек-номера.
    :param deal_id: ID сделки.
    :param track_number: Трек-номер сделки.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE tracked_deals
            SET deal_id = ?
            WHERE track_number = ?
        """, (deal_id, track_number))
        conn.commit()
        logging.info(f"Поле deal_id обновлено для трек-номера {track_number} с новым значением {deal_id}.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при обновлении таблицы tracked_deals: {e}")
    finally:
        conn.close()


def find_deal_by_track(track_number, current_deal_id=None):
    """
    Ищет сделку в локальной базе данных по трек-номеру.
    :param track_number: Трек-номер для поиска.
    :param current_deal_id: ID текущей сделки для исключения из результата.
    :return: Словарь с ID сделки или None, если ничего не найдено.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выполняем запрос к таблице tracked_deals
    cursor.execute("""
        SELECT deal_id
        FROM tracked_deals
        WHERE track_number = ?
    """, (track_number,))

    result = cursor.fetchone()
    conn.close()

    if result:
        deal = {"ID": result[0]}

        # Если найденная сделка совпадает с текущей, игнорируем её
        if str(deal["ID"]) == str(current_deal_id):
            logging.info(f"Найдена только текущая сделка ID {current_deal_id}. Пропускаем.")
            return None

        logging.info(f"Найдена сделка в базе данных с трек-номером {track_number}: {deal}")
        return deal
    else:
        logging.info(f"Сделка с трек-номером {track_number} не найдена в базе данных.")
        return None


# Асинхронные операции
async def delete_deal_by_track_number(track_number):
    """
    Удаляет сделку из базы данных по трек-номеру.
    """
    if not track_number:
        logging.info("Трек номер пуст. Удаление сделки не требуется.")
        return

    logging.info(f"Трек номер для удаления: {track_number}")

    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Проверим, хранится ли трек-номер в базе перед удалением
    cursor.execute('SELECT track_number FROM track_numbers WHERE track_number = ?', (track_number,))
    stored_track_number = cursor.fetchone()

    if stored_track_number:
        # Удаляем запись
        cursor.execute('DELETE FROM track_numbers WHERE track_number = ?', (track_number,))
        conn.commit()
        logging.info(f"Удалена сделка с трек номером: {track_number}")
    else:
        logging.info(f"Сделка с трек номером {track_number} не найдена в базе данных.")

    conn.close()


# Операции с таблицей вебхуков
def save_webhook_to_db(entity_id, event_type):
    """
    Сохраняет данные вебхука в таблицу webhooks.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Текущая метка времени для записи
    timestamp = datetime.utcnow().isoformat()

    # Вставляем запись вебхука в таблицу
    cursor.execute("""
    INSERT INTO webhooks (entity_id, event_type, timestamp, processed)
    VALUES (?, ?, ?, 0)
    """, (entity_id, event_type, timestamp))

    conn.commit()
    conn.close()


def get_latest_webhook_timestamp():
    """
    Получает время последнего поступившего вебхука из базы данных.
    Возвращает datetime объекта или None, если вебхуков нет.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT timestamp
    FROM webhooks
    WHERE processed = 0
    ORDER BY timestamp DESC
    LIMIT 1
    """)

    result = cursor.fetchone()
    conn.close()

    return datetime.fromisoformat(result[0]) if result else None


def mark_webhook_as_processed(webhook_id):
    """
    Помечает вебхук с заданным ID как обработанный.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
    UPDATE webhooks
    SET processed = 1
    WHERE id = ?
    """, (webhook_id,))

    conn.commit()
    conn.close()


def get_unprocessed_webhooks():
    """
    Получает все необработанные вебхуки из базы данных.
    Возвращает список словарей с данными вебхуков.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id, entity_id, event_type, timestamp
    FROM webhooks
    WHERE processed = 0
    ORDER BY timestamp ASC
    """)

    rows = cursor.fetchall()
    conn.close()

    # Преобразуем данные в список словарей
    webhooks = [
        {
            "id": row[0],
            "entity_id": row[1],
            "event_type": row[2],
            "timestamp": row[3]
        }
        for row in rows
    ]

    return webhooks


# Операции с таблицей результатов
def save_processed_result(entity_id, event_type, data):
    """
    Сохраняет обработанные данные вебхука в таблицу processed_results.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Преобразуем данные в JSON-строку для хранения
    data_json = json.dumps(data)
    timestamp = datetime.utcnow().isoformat()

    cursor.execute("""
    INSERT INTO processed_results (entity_id, event_type, data, timestamp, sent)
    VALUES (?, ?, ?, ?, 0)
    """, (entity_id, event_type, data_json, timestamp))

    conn.commit()
    conn.close()


def get_unprocessed_results():
    """
    Получает все необработанные результаты для пакетной отправки.
    Возвращает список словарей с данными обработанных вебхуков.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT id, entity_id, event_type, data, timestamp
    FROM processed_results
    WHERE sent = 0
    ORDER BY timestamp ASC
    """)

    rows = cursor.fetchall()
    conn.close()

    # Преобразуем данные в список словарей
    results = [
        {
            "id": row[0],
            "entity_id": row[1],
            "event_type": row[2],
            "data": json.loads(row[3]),  # Декодируем JSON-строку в словарь
            "timestamp": row[4]
        }
        for row in rows
    ]

    return results


def mark_results_as_processed(result_ids):
    """
    Помечает обработанные результаты как отправленные.
    """
    # Преобразуем значения result_ids к типу int, если это строки, представляющие числа
    processed_ids = []
    for result_id in result_ids:
        try:
            processed_ids.append(int(result_id))
        except (ValueError, TypeError):
            logging.error(f"Некорректный ID: {result_id}, пропуск.")

    # Проверяем, что processed_ids содержит данные после преобразования
    if not processed_ids:
        logging.error("Нет корректных ID для обновления.")
        return

    # Подключение и обновление базы данных
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    try:
        cursor.executemany("""
        UPDATE processed_results
        SET sent = 1
        WHERE id = ?
        """, [(result_id,) for result_id in processed_ids])

        conn.commit()
        logging.info("Результаты успешно помечены как отправленные.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка при обновлении базы данных: {e}")
    finally:
        conn.close()


# Операции с таблицей итоговых сделок
def get_final_deal_from_db(contact_id):
    """
    Извлекает информацию об итоговой сделке для заданного контакта из таблицы final_deals.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Извлекаем последнюю итоговую сделку для указанного контакта
    cursor.execute("""
        SELECT * FROM final_deals WHERE contact_id = ? ORDER BY creation_date DESC LIMIT 1
    """, (contact_id,))
    result = cursor.fetchone()

    conn.close()

    if result:
        return {
            'id': result[0],
            'contact_id': result[1],
            'final_deal_id': result[2],
            'creation_date': result[3],
            'current_stage_id': result[4],
            'track_numbers': result[5],
            'total_weight': result[6],
            'total_amount': result[7],
            'number_of_orders': result[8]
        }
    return None


def save_final_deal_to_db(contact_id, deal_id, creation_date, track_number, current_stage_id, weight=0, amount=0, number_of_orders=1):
    """
    Сохраняет новую итоговую сделку в таблицу final_deals.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Вставляем новую итоговую сделку в таблицу
    cursor.execute("""
        INSERT INTO final_deals (contact_id, final_deal_id, creation_date, current_stage_id, track_numbers, total_weight, total_amount, number_of_orders)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (contact_id, deal_id, creation_date, current_stage_id, track_number, weight, amount, number_of_orders))

    conn.commit()
    conn.close()
    print(f"Сохранена новая итоговая сделка с ID {deal_id} для контакта {contact_id}")


def update_final_deal_in_db(deal_id, track_numbers, stage_id):
    """
    Обновляет информацию об итоговой сделке в таблице final_deals.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Обновляем трек-номера и этап текущей сделки
    cursor.execute("""
        UPDATE final_deals
        SET track_numbers = ?, current_stage_id = ?
        WHERE final_deal_id = ?
    """, (track_numbers, stage_id, deal_id))

    conn.commit()
    conn.close()
    print(f"Обновлена итоговая сделка с ID {deal_id}")


def update_final_deal_id(contact_id, timestamp, new_deal_id):
    """
    Обновляет ID итоговой сделки в таблице final_deals, основываясь на contact_id и временной метке.
    """
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Обновляем ID итоговой сделки
    cursor.execute("""
        UPDATE final_deals
        SET final_deal_id = ?
        WHERE contact_id = ? AND creation_date = ?
    """, (new_deal_id, contact_id, timestamp))

    conn.commit()
    conn.close()
    logging.info(f"Обновлен final_deal_id для контакта {contact_id} на {new_deal_id}")


# Функция для сохранения TASK_ID в базу данных
def save_task_to_db(deal_id, task_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute(
        'INSERT OR REPLACE INTO deal_tasks (deal_id, task_id) VALUES (?, ?)',
        (deal_id, task_id)
    )
    conn.commit()
    conn.close()
    logging.info(f"Сохранен task_id {task_id} для сделки deal_id {deal_id}.")


# Функция для получения TASK_ID по DEAL_ID из базы данных
def get_task_id_by_deal_id(deal_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute(
        'SELECT task_id FROM deal_tasks WHERE deal_id = ?',
        (deal_id,)
    )
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def delete_task_from_db(deal_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute(
        'DELETE FROM deal_tasks WHERE deal_id = ?',
        (deal_id,)
    )
    conn.commit()
    conn.close()
    logging.info(f"Удалена запись из базы данных для сделки deal_id {deal_id}.")


def save_broadcast_message(chat_id, message_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO broadcast_messages (chat_id, message_id)
    VALUES (?, ?)
    """, (chat_id, message_id))

    conn.commit()
    conn.close()


def get_last_broadcast_messages():
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    cursor.execute("""
    SELECT chat_id, message_id FROM broadcast_messages
    ORDER BY id DESC
    """)
    messages = cursor.fetchall()
    conn.close()
    return messages


def save_deal_history(deal_id, track_number, original_date_modify, stage_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO deal_history (deal_id, track_number, original_date_modify, stage_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(track_number) DO UPDATE SET
            original_date_modify = excluded.original_date_modify,
            stage_id = excluded.stage_id
    """, (deal_id, track_number, original_date_modify, stage_id))
    conn.commit()
    conn.close()


def get_original_date_by_track(track_number):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT original_date_modify, stage_id FROM deal_history WHERE track_number = ?
    """, (track_number,))
    result = cursor.fetchone()
    conn.close()
    return result


def delete_client_from_db(phone):
    """
    Удаляет клиента из таблицы `clients` по номеру телефона.
    """
    try:
        conn = sqlite3.connect('clients.db')
        cursor = conn.cursor()
        cursor.execute("DELETE FROM clients WHERE phone = ?", (phone,))
        deleted_rows = cursor.rowcount
        conn.commit()
        conn.close()
        if deleted_rows > 0:
            logging.info(f"Удалена запись из базы данных для телефона {phone}.")
        else:
            logging.warning(f"Запись с телефоном {phone} не найдена в базе данных.")
        return deleted_rows > 0
    except Exception as e:
        logging.error(f"Ошибка при удалении записи с телефоном {phone}: {e}")
        return False
