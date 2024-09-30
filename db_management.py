import sqlite3
import random
import logging


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

    conn.commit()
    conn.close()


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
            "pickup_point": result[6]
        }
    else:
        return None  # Если пользователь не найден, возвращаем None


def check_chat_id_exists(chat_id):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()

    # Выполняем запрос на проверку наличия chat_id
    cursor.execute('SELECT 1 FROM clients WHERE chat_id = ?', (chat_id,))
    result = cursor.fetchone()

    conn.close()

    # Возвращаем True, если запись найдена, иначе False
    return result is not None


# Функция для получения всех chat_id из базы данных
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


# Функции для проверки и присвоения vip номеров
def is_vip_code_available(code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("SELECT vip_code FROM vip_codes WHERE vip_code = ?", (code,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def update_personal_code(old_code, new_code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("UPDATE clients SET personal_code = ? WHERE personal_code = ?", (new_code, old_code))
    updated = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def remove_vip_code(code):
    conn = sqlite3.connect('clients.db')
    cursor = conn.cursor()
    cursor.execute("DELETE FROM vip_codes WHERE vip_code = ?", (code,))
    conn.commit()
    conn.close()


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
