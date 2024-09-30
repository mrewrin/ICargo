from aiogram.fsm.state import State, StatesGroup


class Reg(StatesGroup):
    # Состояния регистрации
    name = State()                  # Состояние для ввода имени пользователя
    phone = State()                 # Состояние для ввода номера телефона пользователя
    city = State()                  # Состояние для выбора города
    pickup_point = State()          # Состояние для выбора пункта выдачи


class Upd(StatesGroup):
    # Состояния регистрации
    name = State()                  # Состояние для обновления имени пользователя
    phone = State()                 # Состояние для обновления номера телефона пользователя
    city = State()                  # Состояние для обновления города
    pickup_point = State()          # Состояние для обновления пункта выдачи


class Menu(StatesGroup):
    # Состояние меню
    main_menu = State()             # Состояние для ожидания нажатия inline кнопки меню
    confirm_update = State()        # Состояние для подтверждения обновления данных при дубликате контакта


class Track(StatesGroup):
    # Состояния ввода трек номера
    add_track = State()             # Состояние для обработки кнопки "Добавить трек номер"
    track_number = State()          # Состояние для ввода трек-номера
    track_name = State()            # Состояние для ввода названия трек-номера
    track_name_update = State()     # Состояние для обновления названия трек-номера


class Package(StatesGroup):
    # Состояния поиска посылки
    find_package = State()          # Состояние для обработки кнопки "Найти посылку"
    phone_search = State()          # Состояние для поиска по номеру телефона


class Instructions(StatesGroup):
    # Состояния инструкций
    address_instructions = State()  # Состояние для обработки кнопки "Инструкция"
    create_instructions = State()   # Состояние для создания инструкций


class Settings(StatesGroup):
    # Состояние настроек
    settings = State()              # Состояние для обработки кнопки "Настройки"
