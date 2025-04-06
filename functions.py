import re
import sqlite3
import pandas as pd
from datetime import datetime
from config import DATABASE_PATH


def transliterate(string):
    """
    Функция для транслитерации имени с кириллицы на латиницу.
    """
    capital_letters = {
        u'А': u'A', u'Б': u'B', u'В': u'V', u'Г': u'G', u'Д': u'D', u'Е': u'E', u'Ё': u'E', u'З': u'Z',
        u'И': u'I', u'Й': u'Y', u'К': u'K', u'Л': u'L', u'М': u'M', u'Н': u'N', u'О': u'O', u'П': u'P',
        u'Р': u'R', u'С': u'S', u'Т': u'T', u'У': u'U', u'Ф': u'F', u'Х': u'H', u'Ъ': u'', u'Ы': u'Y',
        u'Ь': u'', u'Э': u'E',
    }

    capital_letters_transliterated_to_multiple_letters = {
        u'Ж': u'Zh', u'Ц': u'Ts', u'Ч': u'Ch', u'Ш': u'Sh', u'Щ': u'Sch', u'Ю': u'Yu', u'Я': u'Ya',
    }

    lower_case_letters = {
        u'а': u'a', u'б': u'b', u'в': u'v', u'г': u'g', u'д': u'd', u'е': u'e', u'ё': u'e', u'ж': u'zh',
        u'з': u'z', u'и': u'i', u'й': u'y', u'к': u'k', u'л': u'l', u'м': u'm', u'н': u'n', u'о': u'o',
        u'п': u'p', u'р': u'r', u'с': u's', u'т': u't', u'у': u'u', u'ф': u'f', u'х': u'h', u'ц': u'ts',
        u'ч': u'ch', u'ш': u'sh', u'щ': u'sch', u'ъ': u'', u'ы': u'y', u'ь': u'', u'э': u'e', u'ю': u'yu',
        u'я': u'ya',
    }

    capital_and_lower_case_letter_pairs = {}

    for capital_letter, capital_letter_translit in capital_letters_transliterated_to_multiple_letters.items():
        for lowercase_letter, lowercase_letter_translit in lower_case_letters.items():
            capital_and_lower_case_letter_pairs[u"%s%s" % (capital_letter, lowercase_letter)] = u"%s%s" % (
                capital_letter_translit, lowercase_letter_translit)

    for dictionary in (capital_and_lower_case_letter_pairs, capital_letters, lower_case_letters):
        for cyrillic_string, latin_string in dictionary.items():
            string = re.sub(cyrillic_string, latin_string, string)

    for cyrillic_string, latin_string in capital_letters_transliterated_to_multiple_letters.items():
        string = re.sub(cyrillic_string, latin_string.upper(), string)

    return string.upper()


# Функция для форматирования телефона
def format_phone(phone: str) -> str:
    # Удаляем все нечисловые символы
    phone = re.sub(r'\D', '', phone)

    # Если номер начинается с 8, заменяем на 7
    if phone.startswith('8'):
        phone = '7' + phone[1:]

    # Если номер не начинается с 7 или 8, добавляем 7 в начало
    elif not phone.startswith('7'):
        phone = '7' + phone
    return f"+{phone}"


def validate_phone(phone):
    """
    Функция для проверки номера телефона по формату +7XXXXXXXXXX.
    """
    pattern = re.compile(r'^\+7\d{10}$')
    return pattern.match(phone)


def generate_address_instructions(name_cyrillic, personal_code, name_translit, pickup_point_code):
    """
    Генерирует инструкцию по заполнению адреса на основе пункта выдачи.
    """
    instructions = {
        "pv_astana_1": (
            f"🙏🏻 Здравствуйте, {name_cyrillic}, мы рады, что Вы выбрали нас\\!\n\n"
            f"📌 Ваш персональный код:\n"
            f"佳人AST{personal_code}\n\n"
            f"📖 Инструкция по заполнению адреса склада в Китае:\n"
            f"1\\) `佳人AST{personal_code}`\n"
            f"2\\) `18346727700`\n"
            f"3\\) `广东省 佛山市 南海区`\n"
            f"4\\) `丹灶镇金沙银沙南路88号 (佳人AST{personal_code}_{name_translit}_ASTANA+ESIL)`\n\n"
            f"В третьем пункте иероглифы нужно выбрать вручную\\. Остальное можно скопировать\\.\n\n"
            f"📍 Адрес филиала:\n"
            f"ул\\. Кабанбай батыра, 42\n\n"
            f"Ссылка на группу: https://t\\.me/iCargoLife\n"
            f"📞 Вы можете связаться с нами по номеру: 8 \\(700\\) 060\\-10\\-36\n\n"
            f"Ваше обращение не останется без ответа\\. Мы всегда на связи и будем рады помочь Вам по любому вопросу"
        ),
        "pv_astana_2": (
            f"🙏🏻 Здравствуйте, {name_cyrillic}, мы рады, что Вы выбрали нас\\!\n\n"
            f"📌 Ваш персональный код:\n"
            f"领袖AST{personal_code}\n\n"
            f"📖 Инструкция по заполнению адреса склада в Китае:\n"
            f"1\\) `领袖AST{personal_code}`\n"
            f"2\\) `18346727700`\n"
            f"3\\) `广东省 佛山市 南海区`\n"
            f"4\\) `丹灶镇金沙银沙南路88号 (领袖AST{personal_code}_{name_translit}_ASTANA+ALMATINSKIY)`\n\n"
            f"В третьем пункте иероглифы нужно выбрать вручную\\. Остальное можно скопировать\\.\n\n"
            f"📍 Адрес филиала:\n"
            f"ул\\. Кажымукан, 12Б\n\n"
            f"Ссылка на группу: https://t\\.me/iCargoLife\n"
            f"📞 Вы можете связаться с нами по номеру: 8 \\(708\\) 498\\-50\\-58\n\n"
            f"Ваше обращение не останется без ответа\\. Мы всегда на связи и будем рады помочь Вам по любому вопросу"
        ),
        "pv_karaganda_1": (
            f"🙏🏻 Здравствуйте, {name_cyrillic}, мы рады, что Вы выбрали нас\\!\n\n"
            f"📌 Ваш персональный код:\n"
            f"才子KRG{personal_code}\n\n"
            f"📖 Инструкция по заполнению адреса склада в Китае:\n"
            f"1\\) `才子KRG{personal_code}`\n"
            f"2\\) `18346727700`\n"
            f"3\\) `广东省 佛山市 南海区`\n"
            f"4\\) `丹灶镇金沙银沙南路88号 (才子KRG{personal_code}_{name_translit}_KRG+UG)`\n\n"
            f"В третьем пункте иероглифы нужно выбрать вручную\\. Остальное можно скопировать\\.\n\n"
            f"📍 Адрес филиала:\n"
            f"ул\\. Язева 14/1, ТРЦ Проспект \\(Корзина\\), 3 этаж, 24 бутик\n\n"
            f"Ссылка на группу: https://t\\.me/iCargoLife\n"
            f"📞 Вы можете связаться с нами по номеру: 8 \\(776\\) 060\\-10\\-36\n\n"
            f"Ваше обращение не останется без ответа\\. Мы всегда на связи и будем рады помочь Вам по любому вопросу"
        )
    }
    return instructions.get(pickup_point_code, "Пункт выдачи не указан или не поддерживается.")


def export_database_to_excel():
    conn = sqlite3.connect(DATABASE_PATH)

    table_name = "clients"
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)

    output_file = "/data/output.xlsx"
    df.to_excel(output_file, index=False, engine='openpyxl')

    conn.close()
    return output_file


def trim_time_from_iso(iso_str):
    return datetime.fromisoformat(iso_str).date().isoformat() if iso_str else None


def format_date(iso_str: str) -> str:
    try:
        # Возвращаем дату в формате dd.mm.yyyy
        return datetime.fromisoformat(iso_str).strftime("%d.%m.%Y")
    except Exception:
        return "Неизвестная дата"
