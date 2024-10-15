import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.exceptions import ApiError
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import sqlite3
import re
import logging
import datetime
import gspread.utils
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
# Настройка логирования
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Определяем области видимости
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Загружаем учетные данные из переменной среды
json_keyfile = os.getenv('GOOGLE_SHEETS_JSON_KEY')
if json_keyfile is None:
    logging.error("Не установлена переменная среды GOOGLE_SHEETS_JSON_KEY")
    raise ValueError("Не установлена переменная среды GOOGLE_SHEETS_JSON_KEY")

creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(json_keyfile), scope)

# Авторизуемся
client = gspread.authorize(creds)

# Теперь вы можете открыть таблицу по имени или ID
spreadsheet = client.open("Your Spreadsheet Name")
json_keyfile = os.getenv('GOOGLE_SHEETS_JSON_KEY')
creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(json_keyfile), scope)
# Подключение к базе данных SQLite
# Подключение к базе данных SQLite
# Сообщение о создании/открытии базы данных
logging.info("Открытие или создание базы данных 'bindings.db'")
conn = sqlite3.connect('bindings.db')
cursor = conn.cursor()
# Попытка добавить столбец inactive_column, если он отсутствует
try:
    cursor.execute("ALTER TABLE bindings ADD COLUMN inactive_column TEXT DEFAULT 'E'")
    logging.info("Добавлен столбец 'inactive_column' в таблицу 'bindings'")
except sqlite3.OperationalError:
    logging.info("Столбец 'inactive_column' уже существует в таблице 'bindings'")

# Попытка добавить столбцы id_column и answers_column, если они отсутствуют
try:
    cursor.execute("ALTER TABLE bindings ADD COLUMN id_column TEXT DEFAULT 'A'")
    logging.info("Добавлен столбец 'id_column' в таблицу 'bindings'")
except sqlite3.OperationalError:
    logging.info("Столбец 'id_column' уже существует в таблице 'bindings'")

try:
    cursor.execute("ALTER TABLE bindings ADD COLUMN answers_column TEXT DEFAULT 'D'")
    logging.info("Добавлен столбец 'answers_column' в таблицу 'bindings'")
except sqlite3.OperationalError:
    logging.info("Столбец 'answers_column' уже существует в таблице 'bindings'")

# Создание таблицы bindings, если её ещё нет
cursor.execute('''
CREATE TABLE IF NOT EXISTS bindings (
    peer_id INTEGER PRIMARY KEY,
    spreadsheet_id TEXT,
    sheet_name TEXT,
    id_column TEXT DEFAULT 'A',          -- Столбец с ID пользователей (по умолчанию A)
    display_columns TEXT,                -- Список столбцов для отображения статистики (например, "C,D,A")
    points_column TEXT DEFAULT 'B',      -- Столбец для баллов пользователей
    warn_column TEXT DEFAULT 'C',        -- Столбец для предупреждений пользователей
    answers_column TEXT DEFAULT 'D'      -- Столбец для количества ответов
)
''')
logging.info("Проверка существования или создание таблицы 'bindings'")

# Создание таблицы для хранения номера последнего скриншота
cursor.execute('''
CREATE TABLE IF NOT EXISTS screenshot_counter (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    last_number INTEGER DEFAULT 0
)
''')
logging.info("Проверка существования или создание таблицы 'screenshot_counter'")

# Создание таблиц attached_chats и roles
cursor.execute('''
CREATE TABLE IF NOT EXISTS attached_chats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    attached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')
logging.info("Проверка существования или создание таблицы 'attached_chats'")

cursor.execute('''
CREATE TABLE IF NOT EXISTS roles (
    user_id INTEGER PRIMARY KEY,
    role TEXT
)
''')
logging.info("Проверка существования или создание таблицы 'roles'")

# Подтверждение изменений
conn.commit()
logging.info("Изменения подтверждены")

attached_chat = None
def get_current_date():
    return datetime.datetime.now().strftime("%d.%m.%Y")
def get_date_column(sheet, date_str):
    # Предполагается, что заголовки находятся в первой строке
    headers = sheet.row_values(1)
    if date_str in headers:
        return headers.index(date_str) + 1  # gspread использует 1-индексацию
    else:
        # Если колонки нет, создаем её в конце таблицы
        new_col = len(headers) + 1
        sheet.update_cell(1, new_col, date_str)
        return new_col


def handle_unbindscreen_command(peer_id):
    global attached_chat
    if attached_chat == peer_id:
        attached_chat = None  # Отвязываем беседу
        vk.messages.send(
            peer_id=peer_id,
            message="❌ Беседа успешно отвязана от получения скриншотов.",
            random_id=0
        )
        logging.info(f"Беседа {peer_id} отвязана от получения скриншотов.")
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="⚠️ Эта беседа не была привязана к получению скриншотов.",
            random_id=0
        )
def send_start_message(peer_id):
    keyboard = VkKeyboard(one_time=False)  # Клавиатура не исчезает после использования
    keyboard.add_button('Статистика', color=VkKeyboardColor.PRIMARY)  # Кнопка для вызова команды /stats

    vk.messages.send(
        peer_id=peer_id,
        message="Добро пожаловать! Нажмите кнопку ниже для просмотра статистики.",
        random_id=0,
        keyboard=keyboard.get_keyboard()  # Отправляем клавиатуру с кнопкой
    )

def get_next_points_number():
    cursor.execute("SELECT last_number FROM screenshot_counter ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()

    if result:
        current_number = result[0] + 1
    else:
        current_number = 1

    cursor.execute("INSERT INTO screenshot_counter (last_number) VALUES (?)", (current_number,))
    conn.commit()

    return current_number
def set_nektiv_column(peer_id, column, user_role):
    """
    Устанавливает столбец для периода неактивности пользователей.
    Доступно только администраторам.
    """
    # Проверка прав доступа
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="❌ У вас нет прав для выполнения этой команды. Только администраторы могут устанавливать столбцы для неактивности.",
            random_id=0
        )
        logging.warning(f"Пользователь {user_role} попытался использовать команду /nsheet без прав.")
        return

    # Проверка формата столбца
    if not re.match(r'^[A-Z]+$', column.upper()):
        vk.messages.send(
            peer_id=peer_id,
            message="🚫 Неверный формат столбца. Используйте, например, 'C', 'D' и т.д.",
            random_id=0
        )
        logging.warning(f"Неверный формат столбца в команде /nsheet: {column}")
        return

    # Обновление привязки в базе данных
    cursor.execute("""
        UPDATE bindings
        SET inactive_column = ?
        WHERE peer_id = ?
    """, (column.upper(), peer_id))
    conn.commit()

    # Уведомление пользователя об успешном изменении
    vk.messages.send(
        peer_id=peer_id,
        message=f"✅ Столбец для неактивности пользователей успешно установлен на **{column.upper()}**.",
        random_id=0
    )
    logging.info(f"Столбец для неактивности в беседе {peer_id} установлен на {column.upper()}.")

def add_inactivity_period(peer_id, performer_id, mention, start_date, end_date, user_role):
    """
    Добавляет период неактивности пользователю.
    """
    # Проверка прав: только admin может устанавливать неактивность
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        return

    # Проверка формата дат
    try:
        start_date = datetime.datetime.strptime(start_date, "%d.%m.%Y")
        end_date = datetime.datetime.strptime(end_date, "%d.%m.%Y")
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат дат. Используйте формат ДД.ММ.ГГГГ.",
            random_id=0
        )
        return

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, inactive_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        return

    spreadsheet_id, sheet_name, id_column, inactive_column = binding

    if not inactive_column:
        vk.messages.send(
            peer_id=peer_id,
            message="Столбец для неактива не установлен. Используйте команду /nsheet для установки.",
            random_id=0
        )
        return

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        return

    # Получение всех значений столбца ID
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message=f"[id{target_user_id}|Пользователь] не найден в таблице.",
            random_id=0
        )
        return

    # Обновление периода неактивности
    inactivity_period = f"{start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}"
    try:
        inactive_cell = f"{inactive_column}{target_row}"
        sheet.update_acell(inactive_cell, inactivity_period)
        vk.messages.send(
            peer_id=peer_id,
            message=f"Период неактивности [id{target_user_id}|пользователя] установлен: {inactivity_period}.",
            random_id=0
        )
        vk.messages.send(
            user_id=target_user_id,
            message=f"Ващ период неактивности был изменён на {inactivity_period} ",
            random_id=0
        )
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить период неактивности в таблице.",
            random_id=0
        )
# Функция для проверки статуса привязки
def handle_checkscreen_command(peer_id):
    global attached_chat
    if attached_chat == peer_id:
        vk.messages.send(
            peer_id=peer_id,
            message="✅ Эта беседа привязана для получения скриншотов.",
            random_id=0
        )
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="❌ Эта беседа не привязана для получения скриншотов.",
            random_id=0
        )
# Функция для добавления столбцов, если они ещё не существуют
def add_column_if_not_exists(table, column, column_type):
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [info[1] for info in cursor.fetchall()]
    if column not in columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
        conn.commit()
        logging.info(f"Добавлен столбец '{column}' в таблицу {table}.")

add_column_if_not_exists('bindings', 'display_columns', 'TEXT')
add_column_if_not_exists('bindings', 'points_column', 'TEXT')
add_column_if_not_exists('bindings', 'warn_column', 'TEXT')

# Добавляем столбец points_column, если его нет
cursor.execute("PRAGMA table_info(bindings)")
columns = [info[1] for info in cursor.fetchall()]
if 'points_column' not in columns:
    cursor.execute("ALTER TABLE bindings ADD COLUMN points_column TEXT")
    conn.commit()
    logging.info("Добавлен столбец 'points_column' в таблицу bindings.")
# Добавляем столбец warn_column, если его нет
cursor.execute("PRAGMA table_info(bindings)")
columns = [info[1] for info in cursor.fetchall()]
if 'warn_column' not in columns:
    cursor.execute("ALTER TABLE bindings ADD COLUMN warn_column TEXT")
    conn.commit()
    logging.info("Добавлен столбец 'warn_column' в таблицу bindings.")

# Инициализация VK API
vk_session = vk_api.VkApi(token='vk1.a._Yx4344F-hfIDazZGzTuq74J3jZASNh0yA5ssw6sbBEZmWaOyP2I1-NqLsUinUzqQQjVFqG6eOA4BPAspKellBxzVBdkzb5tiepiP_cj8e7cUKHiCp5TvEv8QOOKKhBcm8umYZmz7vxSN7SE744d6psFRP0Zu9r5UK-ghkZcysMvYMqMoV0Vhz6Ml4e92Mla-XSUgBl9AaavG3MH2SjJCA')
longpoll = VkBotLongPoll(vk_session, '227286490')
vk = vk_session.get_api()

# Добавляем пользователя с ID 730323700 как главного админа
cursor.execute("REPLACE INTO roles (user_id, role) VALUES (?, ?)", (730323700, 'admin'))
conn.commit()
logging.info("Главный админ (ID: 730323700) добавлен.")
def get_next_screenshot_number():
    # Проверяем, есть ли запись в таблице
    cursor.execute("SELECT last_number FROM screenshot_counter ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()

    if result:
        current_number = result[0] + 1
    else:
        current_number = 1  # Если запись отсутствует, начинаем с 1

    # Обновляем запись в таблице
    cursor.execute("INSERT INTO screenshot_counter (last_number) VALUES (?)", (current_number,))
    conn.commit()

    return current_number

# Функция для пересылки скриншота администратору и добавления инлайн-кнопок
def handle_norma(peer_id, from_id, message_id):
    admin_id = 730323700  # ID администратора
    screenshot_number = get_next_screenshot_number()  # Получаем номер скриншота

    # Проверяем роль пользователя
    user_role = get_user_role(from_id)

    # Если у пользователя нет роли, отправляем сообщение и выходим
    if user_role is None:
        vk.messages.send(
            peer_id=peer_id,
            message="❌ У вас нет роли для отправки норматива.",
            random_id=0
        )
        return

    # Создаем клавиатуру для администратора
    keyboard = VkKeyboard(inline=True)
    keyboard.add_callback_button(label="+3", color=VkKeyboardColor.POSITIVE,
                                 payload={"action": "add_points", "points": 3, "user_id": from_id, "screenshot_number": screenshot_number})
    keyboard.add_callback_button(label="+5", color=VkKeyboardColor.PRIMARY,
                                 payload={"action": "add_points", "points": 5, "user_id": from_id, "screenshot_number": screenshot_number})
    keyboard.add_callback_button(label="+10", color=VkKeyboardColor.NEGATIVE,
                                 payload={"action": "add_points", "points": 10, "user_id": from_id, "screenshot_number": screenshot_number})
    keyboard.add_callback_button(label="0", color=VkKeyboardColor.SECONDARY,
                                 payload={"action": "add_points", "points": 0, "user_id": from_id, "screenshot_number": screenshot_number})

    mention = f"id{from_id}"

    # Читаем привязанные беседы из БД
    attached_chats = get_attached_chats()

    if attached_chats:
        for chat in attached_chats:
            vk.messages.send(
                peer_id=chat,
                message=f"#{screenshot_number} 🔹 Получен скриншот норматива от [{mention}|пользователя]",
                forward_messages=message_id,  # Пересылаем сообщение с вложением
                keyboard=keyboard.get_keyboard(),
                random_id=0
            )

    # Ответ пользователю
    vk.messages.send(
        peer_id=peer_id,
        message=f"✅ Ваш скриншот успешно отправлен администраторам. Номер: #{screenshot_number}",
        random_id=0
    )

def get_user_id_from_input(input_text):
    # Регулярное выражение для проверки различных форматов ID
    id_pattern = re.compile(r'\[id(\d+)\|.+\]|@id(\d+)|https?://vk.com/id(\d+)')
    nick_pattern = re.compile(r'\[club(\d+)\|.+\]|@club(\d+)|https?://vk.com/club(\d+)')
    link_pattern = re.compile(r'https?://vk.com/([a-zA-Z0-9_]+)')

    match_id = id_pattern.match(input_text)
    match_nick = nick_pattern.match(input_text)
    match_link = link_pattern.match(input_text)

    if match_id:
        return match_id.group(1) or match_id.group(2) or match_id.group(3)
    elif match_nick:
        return match_nick.group(1) or match_nick.group(2) or match_nick.group(3)
    elif match_link:
        return get_user_id_by_nickname(match_link.group(1))
    else:
        return None

def get_user_id_by_nickname(nickname):
    # Эта функция отправляет запрос к API ВКонтакте для получения ID пользователя по нику
    try:
        user_info = vk.method('users.get', {'user_ids': nickname})
        return user_info[0]['id'] if user_info else None
    except Exception as e:
        print(f"Error getting user ID by nickname: {e}")
        return None


def handle_check_binding(peer_id):
    cursor.execute("SELECT peer_id FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()

    if result:
        vk.messages.send(
            peer_id=peer_id,
            message="✅ Беседа привязана для получения скриншотов.",
            random_id=0
        )
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="❌ Беседа не привязана.",
            random_id=0
        )
def get_user_id_by_screen_name(screen_name):
    vk_session = vk_api.VkApi(token='vk1.a._Yx4344F-hfIDazZGzTuq74J3jZASNh0yA5ssw6sbBEZmWaOyP2I1-NqLsUinUzqQQjVFqG6eOA4BPAspKellBxzVBdkzb5tiepiP_cj8e7cUKHiCp5TvEv8QOOKKhBcm8umYZmz7vxSN7SE744d6psFRP0Zu9r5UK-ghkZcysMvYMqMoV0Vhz6Ml4e92Mla-XSUgBl9AaavG3MH2SjJCA')
    vk = vk_session.get_api()

    try:
        user_info = vk.users.get(user_ids=screen_name)
        return user_info[0]['id'] if user_info else None
    except vk_api.exceptions.VkApiError as e:
        print(f"Error retrieving user ID: {e}")
        return None
def set_inactivity_period(peer_id, mention, start_date, end_date, user_role):
    """
    Устанавливает период неактивности для пользователя.
    """
    # Проверка прав: только admin может устанавливать период неактивности
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {peer_id} попытался использовать команду /nektiv без прав.")
        return

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        logging.warning(f"Неверный формат ID в команде /nektiv: {mention}")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, inactive_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не привязана таблица при попытке установить период неактивности.")
        return

    spreadsheet_id, sheet_name, id_column, inactive_column = binding

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение всех значений столбца ID
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message=f"[id{target_user_id}|Пользователь] не найден в таблице.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не найден в таблице {spreadsheet_id}.")
        return

    # Обновление периода неактивности
    try:
        sheet.update_acell(f"{inactive_column}{target_row}", f"{start_date} - {end_date}")
        vk.messages.send(
            peer_id=peer_id,
            message=f"Период неактивности для [id{target_user_id}|пользователя] установлен: {start_date} - {end_date}.",
            random_id=0
        )
        logging.info(f"Период неактивности для [id{target_user_id}|пользователя] установлен: {start_date} - {end_date}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить период неактивности в таблице.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении периода неактивности: {e}")

def set_points_column(peer_id, column, user_role):
    """
    Устанавливает столбец для баллов пользователей.
    Доступно только администраторам.
    """
    # Проверка прав доступа
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="❌ У вас нет прав для выполнения этой команды. Только администраторы могут устанавливать столбцы для баллов.",
            random_id=0
        )
        logging.warning(f"Пользователь {user_role} попытался использовать команду /pointsheet без прав.")
        return

    # Проверка формата столбца
    if not re.match(r'^[A-Z]+$', column.upper()):
        vk.messages.send(
            peer_id=peer_id,
            message="🚫 Неверный формат столбца. Используйте, например, 'C', 'D' и т.д.",
            random_id=0
        )
        logging.warning(f"Неверный формат столбца в команде /pointsheet: {column}")
        return

    # Обновление привязки в базе данных
    cursor.execute("""
        UPDATE bindings
        SET points_column = ?
        WHERE peer_id = ?
    """, (column.upper(), peer_id))
    conn.commit()

    # Уведомление пользователя об успешном изменении
    vk.messages.send(
        peer_id=peer_id,
        message=f"✅ Столбец для баллов пользователей успешно установлен на **{column.upper()}**.",
        random_id=0
    )
    logging.info(f"Столбец для баллов в беседе {peer_id} установлен на {column.upper()}.")
def add_answers(peer_id, performer_id, mention, answers, reason, user_role):
    """
    Добавляет количество ответов пользователю с указанием причины.
    """
    # Проверка прав: только admin и senmoder могут добавлять ответы
    if user_role not in ['admin', 'senmoder']:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {performer_id} попытался использовать команду /ans без прав.")
        return

    # Проверка, что количество ответов является целым числом
    if not str(answers).isdigit():
        vk.messages.send(
            peer_id=peer_id,
            message="Количество ответов должно быть целым числом.",
            random_id=0
        )
        logging.warning(f"Неверный формат количества ответов в команде /ans: {answers}")
        return

    answers = int(answers)

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        logging.warning(f"Неверный формат ID в команде /ans: {mention}")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, answers_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не привязана таблица при попытке добавить ответы.")
        return

    spreadsheet_id, sheet_name, id_column, answers_column = binding

    if not answers_column:
        vk.messages.send(
            peer_id=peer_id,
            message="Столбец для ответов не установлен. Используйте команду /anssheet для установки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не установлен столбец для ответов.")
        return

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение всех значений столбца ID
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message="Пользователь не найден в таблице.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не найден в таблице {spreadsheet_id}.")
        return

    # Получение текущего количества ответов
    try:
        current_answers_cell = f"{answers_column}{target_row}"
        current_answers = sheet.acell(current_answers_cell).value
        current_answers = int(current_answers) if current_answers.isdigit() else 0
    except Exception as e:
        current_answers = 0
        logging.error(f"Ошибка при получении текущих ответов: {e}")

    # Обновление количества ответов
    new_answers = current_answers + answers
    try:
        sheet.update_acell(current_answers_cell, new_answers)
        logging.info(f"Количество ответов пользователя ID {target_user_id} обновлено с {current_answers} на {new_answers}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить количество ответов в таблице.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении ответов: {e}")
        return

    # Уведомление пользователя
    try:
        vk.messages.send(
            user_id=target_user_id,
            message=f"Вам добавлено {answers} ответов.\nПричина: {reason}\nВаше текущее количество ответов: {new_answers}.",
            random_id=0
        )
        logging.info(f"Уведомление о добавлении ответов отправлено пользователю ID {target_user_id}.")
    except ApiError as e:
        if e.code == 901:
            logging.warning(f"Не удалось отправить сообщение пользователю {target_user_id}: {e}")
        else:
            logging.error(f"Произошла ошибка при отправке сообщения пользователю {target_user_id}: {e}")

    # Уведомление в беседу
    vk.messages.send(
        peer_id=peer_id,
        message=f"Пользователю ID {target_user_id} добавлено {answers} ответов.\nПричина: {reason}\nТекущие ответы: {new_answers}",
        random_id=0
    )
def set_answers_column(peer_id, column, user_role):
    """
    Устанавливает столбец для ответов пользователей.
    Доступно только администраторам.
    """
    # Проверка прав доступа
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="❌ У вас нет прав для выполнения этой команды. Только администраторы могут устанавливать столбцы для ответов.",
            random_id=0
        )
        logging.warning(f"Пользователь {user_role} попытался использовать команду /anssheet без прав.")
        return

    # Проверка формата столбца
    if not re.match(r'^[A-Z]+$', column.upper()):
        vk.messages.send(
            peer_id=peer_id,
            message="🚫 Неверный формат столбца. Используйте, например, 'C', 'D' и т.д.",
            random_id=0
        )
        logging.warning(f"Неверный формат столбца в команде /anssheet: {column}")
        return

    # Обновление привязки в базе данных
    cursor.execute("""
        UPDATE bindings
        SET answers_column = ?
        WHERE peer_id = ?
    """, (column.upper(), peer_id))
    conn.commit()

    # Уведомление пользователя об успешном изменении
    vk.messages.send(
        peer_id=peer_id,
        message=f"✅ Столбец для ответов пользователей успешно установлен на **{column.upper()}**.",
        random_id=0
    )
    logging.info(f"Столбец для ответов в беседе {peer_id} установлен на {column.upper()}.")

def set_warn_column(peer_id, column, user_role):
    """
    Устанавливает столбец для хранения выговоров пользователей.
    Доступно только администраторам.
    """
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь с ролью {user_role} попытался использовать команду /wsheet без прав.")
        return

    if not re.match(r'^[A-Z]+$', column.upper()):
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат столбца. Используйте, например, 'C', 'D' и т.д.",
            random_id=0
        )
        logging.warning(f"Неверный формат столбца в команде /wsheet: {column}")
        return

    cursor.execute("""
        UPDATE bindings
        SET warn_column = ?
        WHERE peer_id = ?
    """, (column.upper(), peer_id))
    conn.commit()
    vk.messages.send(
        peer_id=peer_id,
        message=f"Столбец для выговоров пользователей установлен на {column.upper()}.",
        random_id=0
    )
    logging.info(f"Столбец для выговоров в беседе {peer_id} установлен на {column.upper()}.")

def warn_user(peer_id, performer_id, mention, reason, user_role):
    """
    Выдает выговор пользователю с указанием причины.
    """
    # Проверка прав: только admin и senmoder могут выдавать выговоры
    if user_role not in ['admin', 'senmoder']:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {performer_id} попытался использовать команду /warn без прав.")
        return

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        logging.warning(f"Неверный формат ID в команде /warn: {mention}")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, warn_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не привязана таблица при попытке выдать выговор.")
        return

    spreadsheet_id, sheet_name, id_column, warn_column = binding

    if not warn_column:
        vk.messages.send(
            peer_id=peer_id,
            message="Столбец для выговоров не установлен. Используйте команду /wsheet для установки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не установлен столбец для выговоров.")
        return

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение всех значений столбца ID
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message="Пользователь не найден в таблице.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не найден в таблице {spreadsheet_id}.")
        return

    # Получение текущих выговоров
    try:
        current_warn_cell = f"{warn_column}{target_row}"
        current_warn = sheet.acell(current_warn_cell).value
        # Предполагается формат "x/3", где x - количество выговоров
        if current_warn:
            match = re.match(r'(\d+)/(\d+)', current_warn)
            if match:
                current_warn_count = int(match.group(1))
                max_warn = int(match.group(2))
            else:
                current_warn_count = 0
                max_warn = 3  # Значение по умолчанию
        else:
            current_warn_count = 0
            max_warn = 3  # Значение по умолчанию
    except Exception as e:
        current_warn_count = 0
        max_warn = 3
        logging.error(f"Ошибка при получении текущих выговоров: {e}")

    # Проверка на достижение максимального количества выговоров
    if current_warn_count >= max_warn:
        vk.messages.send(
            peer_id=peer_id,
            message=f"Пользователь уже достиг максимального количества выговоров: {current_warn_count}/{max_warn}.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} уже имеет {current_warn_count}/{max_warn} выговоров.")
        return

    # Увеличение выговоров
    new_warn_count = current_warn_count + 1
    new_warn = f"{new_warn_count}/{max_warn}"

    try:
        sheet.update_acell(current_warn_cell, new_warn)
        logging.info(f"Выговоры пользователя ID {target_user_id} обновлены с {current_warn} на {new_warn}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить выговоры в таблице.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении выговоров: {e}")
        return

    # Отправка уведомления пользователю
    try:
        vk.messages.send(
            user_id=target_user_id,
            message=f"Вам был выдан выговор.\nПричина: {reason}\nТекущее количество выговоров: {new_warn}.",
            random_id=0
        )
        logging.info(f"Уведомление о выговоре отправлено пользователю ID {target_user_id}.")
    except ApiError as e:
        if e.code == 901:  # Пользователь закрыл личные сообщения
            logging.warning(f"Не удалось отправить сообщение пользователю {target_user_id}: {e}")
        else:
            logging.error(f"Произошла ошибка при отправке сообщения пользователю {target_user_id}: {e}")

    # Уведомление в беседу
    vk.messages.send(
        peer_id=peer_id,
        message=f"Пользователю ID {target_user_id} выдан выговор.\nПричина: {reason}\nТекущее количество выговоров: {new_warn}.",
        random_id=0
    )

    # Дополнительные действия при достижении максимума выговоров
    if new_warn_count >= max_warn:
        # Например, бан пользователя
        try:
            vk.groups.ban(
                group_id='227286490',  # Убедитесь, что используете правильный group_id
                user_id=target_user_id,
                end_date=0  # Бессрочный бан; измените по необходимости
            )
            vk.messages.send(
                peer_id=peer_id,
                message=f"Пользователь ID {target_user_id} был заблокирован за превышение количества выговоров.",
                random_id=0
            )
            logging.info(f"Пользователь ID {target_user_id} был заблокирован за {new_warn_count}/{max_warn} выговоров.")
        except ApiError as e:
            vk.messages.send(
                peer_id=peer_id,
                message=f"Не удалось заблокировать пользователя ID {target_user_id}.",
                random_id=0
            )
            logging.error(f"Ошибка при блокировке пользователя {target_user_id}: {e}")

def extract_user_id(mention):
    """
    Извлекает user_id из упоминания в формате @id123456 или числового ID.
    """
    if mention.startswith('@id'):
        return int(mention[3:])
    elif mention.isdigit():
        return int(mention)
    else:
        raise ValueError("Неверный формат упоминания.")
# Функция для привязки беседы и сохранения в БД
def handle_getscreen_command(peer_id, from_id):
    # Проверка, существует ли уже привязка для этой беседы
    cursor.execute("SELECT peer_id FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()

    if result:
        # Если привязка уже существует, обновляем ее
        vk.messages.send(
            peer_id=peer_id,
            message="✅ Эта беседа уже привязана для получения скринов.",
            random_id=0
        )
    else:
        # Если привязки нет, создаем новую
        cursor.execute("INSERT INTO bindings (peer_id) VALUES (?)", (peer_id,))
        vk.messages.send(
            peer_id=peer_id,
            message="✅ Беседа успешно привязана для получения скринов.",
            random_id=0
        )

    conn.commit()

# Функция для получения привязанной беседы из БД
def get_attached_chats():
    cursor.execute("SELECT peer_id FROM bindings")
    chats = cursor.fetchall()
    return [chat[0] for chat in chats]  # Возвращаем список всех привязанных бесед
def get_current_balance(user_id):
    # Извлекаем баланс из Google Таблиц или базы данных
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, points_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()

    spreadsheet_id, sheet_name, id_column, points_column = binding

    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == user_id:
                target_row = idx
                break

    if target_row:
        current_points_cell = f"{points_column}{target_row}"
        current_points = sheet.acell(current_points_cell).value
        return int(current_points) if current_points.isdigit() else 0
    else:
        return 0
def add_points(peer_id, performer_id, mention, points, reason, user_role, send_notification=True):
    """
    Добавляет баллы пользователю с указанием причины.
    Баллы вставляются в общую ячейку и в ячейку с текущей датой.
    """
    # Проверка прав: только admin и senmoder могут добавлять баллы
    if user_role not in ['admin', 'senmoder']:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {performer_id} попытался использовать команду /point без прав.")
        return

    # Проверка, что количество баллов является целым числом
    if not str(points).isdigit():
        vk.messages.send(
            peer_id=peer_id,
            message="Количество баллов должно быть целым числом.",
            random_id=0
        )
        logging.warning(f"Неверный формат баллов в команде /point: {points}")
        return

    points = int(points)

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        logging.warning(f"Неверный формат ID в команде /point: {mention}")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, points_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не привязана таблица при попытке добавить баллы.")
        return

    spreadsheet_id, sheet_name, id_column, points_column = binding

    if not points_column:
        vk.messages.send(
            peer_id=peer_id,
            message="Столбец для баллов не установлен. Используйте команду /psheet для установки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не установлен столбец для баллов.")
        return

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение текущей даты
    current_date = get_current_date()

    # Получение колонки с текущей датой
    date_col = get_date_column(sheet, current_date)

    # Получение всех значений столбца ID
    id_col_letter = id_column.upper()
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_col_letter}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message=f"[id{target_user_id}|Пользователь] не найден в таблице.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не найден в таблице {spreadsheet_id}.")
        return

    # Получение текущих баллов в общей ячейке
    try:
        current_points_cell = f"{points_column}{target_row}"
        current_points = sheet.acell(current_points_cell).value
        current_points = int(current_points) if current_points and current_points.isdigit() else 0
    except Exception as e:
        current_points = 0
        logging.error(f"Ошибка при получении текущих баллов: {e}")

    # Обновление баллов в общей ячейке
    new_points = current_points + points
    try:
        sheet.update_acell(current_points_cell, new_points)
        logging.info(f"✨ Баллы [id{target_user_id}|пользователя] обновлены с {current_points} на {new_points} в ячейке {current_points_cell}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить баллы в общей ячейке.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении баллов в общей ячейке: {e}")
        return

    # Получение текущих баллов в ячейке с датой
    try:
        current_date_cell = gspread.utils.rowcol_to_a1(target_row, date_col)
        current_date_points = sheet.acell(current_date_cell).value
        current_date_points = int(current_date_points) if current_date_points and current_date_points.isdigit() else 0
    except Exception as e:
        current_date_points = 0
        logging.error(f"Ошибка при получении текущих баллов в ячейке с датой: {e}")

    # Обновление баллов в ячейке с датой
    new_date_points = current_date_points + points
    try:
        sheet.update_acell(current_date_cell, new_date_points)
        logging.info(f"✨ Баллы [id{target_user_id}|пользователя] обновлены с {current_date_points} на {new_date_points} в ячейке {current_date_cell}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить баллы в ячейке с текущей датой.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении баллов в ячейке с датой: {e}")
        return

    # Отправка уведомления пользователю (в личные сообщения)
    try:
        vk.messages.send(
            user_id=target_user_id,
            message=f"Вам было начислено {points} баллов.\nПричина: {reason}\nВаш текущий баланс: {new_points} баллов.",
            random_id=0
        )
        logging.info(f"Уведомление о начислении баллов отправлено пользователю ID {target_user_id}.")
    except ApiError as e:
        if e.code == 901:  # Пользователь закрыл личные сообщения
            logging.warning(f"Не удалось отправить сообщение пользователю {target_user_id}: {e}")
        else:
            logging.error(f"Произошла ошибка при отправке сообщения пользователю {target_user_id}: {e}")

    # Уведомление в беседу (если включено)
    if send_notification:
        vk.messages.send(
            peer_id=peer_id,
            message=f"✨ Баллы [id{target_user_id}|пользователя] начислено {points} баллов на дату {current_date}.\nПричина: {reason}\nТекущий баланс: {new_points}",
            random_id=0
        )

# Функция для назначения ролей
def assign_role(performer_role, user_id, role, peer_id):
    """
    performer_role: роль пользователя, выполняющего команду
    user_id: ID пользователя, которому назначается роль
    role: роль, которая назначается
    peer_id: ID беседы для уведомления
    """
    # Проверка прав назначения ролей
    if performer_role == 'admin':
        allowed_roles = ['admin', 'senmoder', 'moder']
    elif performer_role == 'senmoder':
        allowed_roles = ['moder']
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для назначения ролей.",
            random_id=0
        )
        return

    if role not in allowed_roles:
        vk.messages.send(
            peer_id=peer_id,
            message=f"Вы не можете назначать роль {role}.",
            random_id=0
        )
        return

    # Назначение роли
    cursor.execute("REPLACE INTO roles (user_id, role) VALUES (?, ?)", (user_id, role))
    conn.commit()

    role_message = {
        'admin': 'Вы были назначены администратором.',
        'senmoder': 'Вы были назначены старшим модератором.',
        'moder': 'Вы были назначены модератором.'
    }

    try:
        vk.messages.send(
            user_id=user_id,
            message=role_message.get(role, 'Ваша роль обновлена.'),
            random_id=0
        )
    except ApiError as e:
        logging.warning(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
        vk.messages.send(
            peer_id=peer_id,
            message=f"Не удалось отправить сообщение пользователю {user_id}.",
            random_id=0
        )

    vk.messages.send(
        peer_id=peer_id,
        message=f"Пользователь с ID {user_id} был назначен {role}.",
        random_id=0
    )
    logging.info(f"Назначена роль {role} пользователю {user_id}.")

# Функция для удаления ролей
def remove_role(performer_role, target_role, peer_id):
    """
    performer_role: роль пользователя, выполняющего команду
    target_role: роль, которую нужно удалить
    peer_id: ID беседы для уведомления
    """
    # Определяем, какие роли может удалять текущий пользователь
    if performer_role == 'admin':
        removable_roles = ['senmoder', 'moder']
    elif performer_role == 'senmoder':
        removable_roles = ['moder']
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для снятия ролей.",
            random_id=0
        )
        return False

    if target_role not in removable_roles:
        vk.messages.send(
            peer_id=peer_id,
            message=f"Вы не можете снять роль {target_role}.",
            random_id=0
        )
        return False

    return True

# Функция для удаления роли пользователя
def delete_role(user_id, peer_id):
    try:
        vk.messages.send(
            user_id=user_id,
            message="Ваша роль была снята.",
            random_id=0
        )
        logging.info(f"Уведомлено о снятии роли пользователя {user_id}.")
    except ApiError as e:
        if e.code == 901:
            logging.warning(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
        else:
            logging.error(f"Произошла непредвиденная ошибка при отправке сообщения пользователю {user_id}: {e}")

    # Удаление роли из базы данных
    cursor.execute("DELETE FROM roles WHERE user_id = ?", (user_id,))
    conn.commit()
    vk.messages.send(
        peer_id=peer_id,
        message=f"Роль пользователя с ID {user_id} была успешно снята.",
        random_id=0
    )
    logging.info(f"Снята роль пользователя {user_id}.")


# Функция для проверки роли пользователя
def get_user_role(user_id):
    cursor.execute("SELECT role FROM roles WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

# Функция для привязки таблицы
def bind_table(peer_id, table_id):
    cursor.execute("SELECT spreadsheet_id FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()
    if result:
        vk.messages.send(
            peer_id=peer_id,
            message=f"Таблица уже привязана: {result[0]}. Вы не можете привязать другую таблицу.",
            random_id=0
        )
    else:
        cursor.execute("INSERT INTO bindings (peer_id, spreadsheet_id) VALUES (?, ?)", (peer_id, table_id))
        conn.commit()
        vk.messages.send(
            peer_id=peer_id,
            message=f"Google таблица {table_id} успешно привязана.",
            random_id=0
        )
        logging.info(f"Привязана таблица {table_id} к беседе {peer_id}.")

# Функция для синхронизации листа
def sync_sheet(peer_id, sheet_name):
    cursor.execute("SELECT spreadsheet_id FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()
    if result:
        spreadsheet_id = result[0]
        try:
            sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
            cursor.execute("UPDATE bindings SET sheet_name = ? WHERE peer_id = ?", (sheet_name, peer_id))
            conn.commit()
            vk.messages.send(
                peer_id=peer_id,
                message=f"Лист {sheet_name} успешно привязан.",
                random_id=0
            )
            logging.info(f"Привязан лист {sheet_name} к таблице {spreadsheet_id} в беседе {peer_id}.")
        except gspread.exceptions.WorksheetNotFound:
            vk.messages.send(
                peer_id=peer_id,
                message=f"Лист {sheet_name} не найден в таблице.",
                random_id=0
            )
            logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="Сначала привяжите Google таблицу командой /bind.",
            random_id=0
        )

# Функция для отвязки таблицы
def unbind_table(peer_id):
    cursor.execute("SELECT spreadsheet_id FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()
    if result:
        cursor.execute("DELETE FROM bindings WHERE peer_id = ?", (peer_id,))
        conn.commit()
        vk.messages.send(
            peer_id=peer_id,
            message="Таблица успешно отвязана.",
            random_id=0
        )
        logging.info(f"Отвязана таблица {result[0]} от беседы {peer_id}.")
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="Таблица не привязана.",
            random_id=0
        )

# Функция для получения информации о таблице и листе
def sheet_info(peer_id):
    cursor.execute("SELECT spreadsheet_id, sheet_name FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()
    if result:
        spreadsheet_id, sheet_name = result
        if sheet_name:
            vk.messages.send(
                peer_id=peer_id,
                message=f"Текущая таблица: {spreadsheet_id}\nЛист: {sheet_name}.",
                random_id=0
            )
            logging.info(f"Отправлена информация о таблице {spreadsheet_id} и листе {sheet_name} в беседу {peer_id}.")
        else:
            vk.messages.send(
                peer_id=peer_id,
                message=f"📊 *Текущая таблица:* {spreadsheet_id}\n Лист не установлен.*",
                random_id=0
            )
            logging.info(f"Отправлена информация о таблице {spreadsheet_id} без привязанного листа в беседу {peer_id}.")
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="⚠️ Таблица не привязана.",
            random_id=0
        )
        logging.info(f"В беседе {peer_id} нет привязанной таблицы.")
def remove_warn(peer_id, performer_id, mention, reason, user_role):
    """
    Уменьшает количество выговоров у пользователя.
    """
    # Проверка прав: только admin и senmoder могут снимать выговоры
    if user_role not in ['admin', 'senmoder']:
        vk.messages.send(
            peer_id=peer_id,
            message="❌ У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {performer_id} попытался использовать команду /unwarn без прав.")
        return

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="🚫 Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        logging.warning(f"Неверный формат ID в команде /unwarn: {mention}")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, warn_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="⚠️ Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не привязана таблица при попытке снять выговор.")
        return

    spreadsheet_id, sheet_name, id_column, warn_column = binding

    if not warn_column:
        vk.messages.send(
            peer_id=peer_id,
            message="⚠️ Столбец для выговоров не установлен. Используйте команду /wsheet для установки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не установлен столбец для выговоров.")
        return

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="🚫 Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение всех значений столбца ID
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message="🚫 Пользователь не найден в таблице.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не найден в таблице {spreadsheet_id}.")
        return

    # Получение текущих выговоров
    try:
        current_warn_cell = f"{warn_column}{target_row}"
        current_warn = sheet.acell(current_warn_cell).value
        # Предполагается формат "x/3", где x - количество выговоров
        if current_warn:
            match = re.match(r'(\d+)/(\d+)', current_warn)
            if match:
                current_warn_count = int(match.group(1))
                max_warn = int(match.group(2))
            else:
                current_warn_count = 0
                max_warn = 3  # Значение по умолчанию
        else:
            current_warn_count = 0
            max_warn = 3  # Значение по умолчанию
    except Exception as e:
        current_warn_count = 0
        max_warn = 3
        logging.error(f"Ошибка при получении текущих выговоров: {e}")

    # Проверка на наличие выговоров
    if current_warn_count <= 0:
        vk.messages.send(
            peer_id=peer_id,
            message=f"⚠️ У пользователя нет выговоров для снятия.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не имеет выговоров.")
        return

    # Уменьшение выговоров
    new_warn_count = current_warn_count - 1
    new_warn = f"{new_warn_count}/{max_warn}"

    try:
        sheet.update_acell(current_warn_cell, new_warn)
        logging.info(f"Выговоры пользователя ID {target_user_id} обновлены с {current_warn} на {new_warn}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="❌ Не удалось обновить выговоры в таблице.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении выговоров: {e}")
        return

    # Отправка уведомления пользователю
    try:
        vk.messages.send(
            user_id=target_user_id,
            message=f"✅ С вашего аккаунта снят один выговор.\nПричина: {reason}\nТекущее количество выговоров: {new_warn}.",
            random_id=0
        )
        logging.info(f"Уведомление о снятии выговора отправлено пользователю ID {target_user_id}.")
    except ApiError as e:
        if e.code == 901:  # Пользователь закрыл личные сообщения
            logging.warning(f"Не удалось отправить сообщение пользователю {target_user_id}: {e}")
        else:
            logging.error(f"Произошла ошибка при отправке сообщения пользователю {target_user_id}: {e}")

    # Уведомление в беседу
    vk.messages.send(
        peer_id=peer_id,
        message=f"✅ У пользователя ID {target_user_id} снят один выговор.\nПричина: {reason}\nТекущее количество выговоров: {new_warn}.",
        random_id=0
    )
def set_id_column(peer_id, column, user_role):
    """
    Устанавливает столбец, в котором бот будет искать ссылки на VK пользователей.
    Доступно только администраторам.
    """
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {user_role} попытался использовать команду /idsheet без прав.")
        return

    if not re.match(r'^[A-Z]+$', column.upper()):
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат столбца. Используйте, например, 'A', 'B' и т.д.",
            random_id=0
        )
        logging.warning(f"Неверный формат столбца в команде /idsheet: {column}")
        return

    cursor.execute("""
        UPDATE bindings
        SET id_column = ?
        WHERE peer_id = ?
    """, (column.upper(), peer_id))
    conn.commit()
    vk.messages.send(
        peer_id=peer_id,
        message=f"Столбец для ID пользователей установлен на {column.upper()}.",
        random_id=0
    )
    logging.info(f"Столбец для ID пользователей в беседе {peer_id} установлен на {column.upper()}.")


def set_menu_ranges(peer_id, columns, user_role):
    """
    Устанавливает список столбцов для отображения статистики, например: A, B, C.
    """
    if user_role != 'admin':
        vk.messages.send(peer_id=peer_id, message="У вас нет прав для выполнения этой команды.", random_id=0)
        logging.warning(f"Пользователь {user_role} попытался использовать команду /menu без прав.")
        return

    # Проверка правильности формата введённых столбцов
    for col in columns:
        if not re.match(r'^[A-Z]+$', col.upper()):
            vk.messages.send(
                peer_id=peer_id,
                message=f"Неверный формат столбца '{col}'. Используйте, например, 'B' или 'C'.",
                random_id=0
            )
            logging.warning(f"Неверный формат столбца '{col}' в команде /menu.")
            return

    # Обновление столбцов в базе данных
    display_columns = ','.join([col.upper() for col in columns])

    cursor.execute("""
        UPDATE bindings
        SET display_columns = ?
        WHERE peer_id = ?
    """, (display_columns, peer_id))
    conn.commit()

    vk.messages.send(
        peer_id=peer_id,
        message=f"Диапазон отображаемых столбцов установлен: {display_columns}.",
        random_id=0
    )
    logging.info(f"Диапазон отображаемых столбцов в беседе {peer_id} установлен: {display_columns}.")


def column_letter_to_index(column_letter):
    """
    Преобразует буквенное обозначение столбца (например, 'A') в 0-индексированный номер столбца.
    """
    try:
        return gspread.utils.a1_to_rowcol(f"{column_letter}1")[1] - 1  # Преобразование в 0-индексированный
    except Exception as e:
        logging.error(f"Ошибка преобразования столбца '{column_letter}': {e}")
        return None


def get_stats(peer_id, user_id, user_role, requester_role):
    """
    Получает статистику для указанного пользователя.
    """
    # Проверка прав доступа
    if requester_role not in ['admin', 'senmoder'] and user_id != from_id:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для просмотра статистики других пользователей.",
            random_id=0
        )
        logging.warning(f"Пользователь {from_id} попытался получить статистику другого пользователя.")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, display_columns FROM bindings WHERE peer_id = ?",
                   (peer_id,))
    binding = cursor.fetchone()

    # Если привязка не найдена, используем значения по умолчанию
    if not binding:
        spreadsheet_id = '1JiYbkshxYVIwaEiCJLkQQGYryKhovvYE1V0tAPjHgVc'
        sheet_name = 'IMPORT'
        id_column = 'A'
        display_columns = 'B,C,D,E,F,G,H,I,J,K,L,M'  # Замените на нужные столбцы по умолчанию, если необходимо
    else:
        spreadsheet_id, sheet_name, id_column, display_columns = binding

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение всех значений таблицы
    all_values = sheet.get_all_values()
    if not all_values:
        vk.messages.send(
            peer_id=peer_id,
            message="Таблица пуста.",
            random_id=0
        )
        logging.warning(f"Таблица {spreadsheet_id} пуста.")
        return

    headers = all_values[0]
    data_rows = all_values[1:]

    # Преобразование id_column из буквенного обозначения в индекс
    id_col_index = column_letter_to_index(id_column.upper())
    if id_col_index is None:
        vk.messages.send(
            peer_id=peer_id,
            message=f"Неверный формат столбца ID: '{id_column}'.",
            random_id=0
        )
        logging.warning(f"Неверный формат столбца ID: '{id_column}'.")
        return

    # Поиск строки с нужным user_id
    target_row = None
    for idx, row in enumerate(data_rows, start=2):
        if id_col_index >= len(row):
            continue  # Пропустить, если строки короче
        cell = row[id_col_index]
        match = re.search(r'id(\d+)', cell)
        if match:
            extracted_id = int(match.group(1))
            if extracted_id == user_id:
                target_row = row
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message=f"Статистика для [id{user_id}|пользователя] не найдена в таблице.",
            random_id=0
        )
        logging.info(f"Статистика для [id{user_id}|пользователя] не найдена в таблице.")
        return

    # Получение индексов столбцов для отображения
    column_indexes = []
    if display_columns:
        for col in display_columns.split(','):
            col = col.strip().upper()
            col_index = column_letter_to_index(col)
            if col_index is not None:
                column_indexes.append(col_index)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message=f"Неверный формат столбца '{col}'.",
                    random_id=0
                )
                logging.warning(f"Неверный формат столбца '{col}'.")
                return

    # Формирование сообщения со статистикой
    stats_message = f"Статистика [id{user_id}|пользователя]:\n"
    for col_index in column_indexes:
        if col_index < len(headers):
            header = headers[col_index]
            value = target_row[col_index] if col_index < len(target_row) else "—"
            stats_message += f"{header}: {value}\n"
        else:
            stats_message += f"Столбец {col_index + 1}: —\n"

    vk.messages.send(
        peer_id=peer_id,
        message=stats_message,
        random_id=0
    )
    logging.info(f"Отправлена статистика для пользователя ID {user_id} в беседу {peer_id}.")


# Функция для получения списка сотрудников
def get_staff_list(peer_id):
    cursor.execute("SELECT user_id, role FROM roles WHERE role IN ('admin', 'senmoder', 'moder')")
    staff = cursor.fetchall()

    # Предустановленные заголовки ролей
    staff_message = "👥 *Список сотрудников:*\n\n"

    roles = {
        'admin': '👑 Администратор:\n',
        'senmoder': '⭐ Старший модератор:\n',
        'moder': '🛡 Модератор:\n'
    }

    # Добавляем значение по умолчанию для каждого типа роли
    staff_dict = {
        'admin': [],
        'senmoder': [],
        'moder': []
    }

    if staff:
        # Разделение пользователей по ролям
        for user_id, role in staff:
            user_info = vk.users.get(user_ids=user_id)
            user_name = f"[id{user_id}|{user_info[0]['first_name']} {user_info[0]['last_name']}]"
            staff_dict[role].append(f"🔹 {user_name}")

        # Формируем сообщение с сотрудниками по каждой роли
        for role, role_header in roles.items():
            staff_message += role_header
            if staff_dict[role]:
                staff_message += '\n'.join(staff_dict[role]) + '\n\n'
            else:
                staff_message += "🔹 Не установлено\n\n"
    else:
        # Если нет сотрудников вообще
        staff_message += "🚫 Сотрудников пока нет."

    # Отправка сообщения
    vk.messages.send(
        peer_id=peer_id,
        message=staff_message,
        random_id=0
    )
    logging.info(f"Отправлен список сотрудников в беседу {peer_id}.")


# Функция для отвязки беседы
def handle_unbind_command(peer_id):
    # Проверяем, привязана ли беседа
    cursor.execute("SELECT peer_id FROM bindings WHERE peer_id = ?", (peer_id,))
    result = cursor.fetchone()

    if result:
        # Если привязана, удаляем привязку
        cursor.execute("DELETE FROM bindings WHERE peer_id = ?", (peer_id,))
        conn.commit()
        vk.messages.send(
            peer_id=peer_id,
            message="✅ Беседа успешно отвязана.",
            random_id=0
        )
    else:
        vk.messages.send(
            peer_id=peer_id,
            message="❌ Беседа не была привязана.",
            random_id=0
        )
def unset_points_column(peer_id, user_role):
    """
    Сбрасывает столбец, в котором хранятся баллы пользователей.
    Доступно только администраторам.
    """
    if user_role != 'admin':
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {user_role} попытался использовать команду /unpointsheet без прав.")
        return

    cursor.execute("""
        UPDATE bindings
        SET points_column = NULL
        WHERE peer_id = ?
    """, (peer_id,))
    conn.commit()
    vk.messages.send(
        peer_id=peer_id,
        message="Столбец для баллов пользователей успешно отвязан.",
        random_id=0
    )
    logging.info(f"Столбец для баллов в беседе {peer_id} был отвязан.")
def remove_points(peer_id, performer_id, mention, points, user_role):
    """
    Уменьшает баллы у пользователя.
    """
    # Проверка прав: только admin и senmoder могут уменьшать баллы
    if user_role not in ['admin', 'senmoder']:
        vk.messages.send(
            peer_id=peer_id,
            message="У вас нет прав для выполнения этой команды.",
            random_id=0
        )
        logging.warning(f"Пользователь {performer_id} попытался использовать команду /unpoint без прав.")
        return

    # Проверка, что количество баллов является целым числом
    if not points.isdigit():
        vk.messages.send(
            peer_id=peer_id,
            message="Количество баллов должно быть целым числом.",
            random_id=0
        )
        logging.warning(f"Неверный формат баллов в команде /unpoint: {points}")
        return

    points = int(points)

    # Извлечение user_id из упоминания
    try:
        target_user_id = extract_user_id(mention)
    except ValueError:
        vk.messages.send(
            peer_id=peer_id,
            message="Неверный формат ID. Используйте @id или числовой ID.",
            random_id=0
        )
        logging.warning(f"Неверный формат ID в команде /unpoint: {mention}")
        return

    # Получение привязки таблицы
    cursor.execute("SELECT spreadsheet_id, sheet_name, id_column, points_column FROM bindings WHERE peer_id = ?", (peer_id,))
    binding = cursor.fetchone()
    if not binding:
        vk.messages.send(
            peer_id=peer_id,
            message="Google таблица не привязана. Используйте команду /bind для привязки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не привязана таблица при попытке уменьшить баллы.")
        return

    spreadsheet_id, sheet_name, id_column, points_column = binding

    if not points_column:
        vk.messages.send(
            peer_id=peer_id,
            message="Столбец для баллов не установлен. Используйте команду /psheet для установки.",
            random_id=0
        )
        logging.warning(f"В беседе {peer_id} не установлен столбец для баллов.")
        return

    try:
        sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        vk.messages.send(
            peer_id=peer_id,
            message="Указанный лист не найден в таблице.",
            random_id=0
        )
        logging.warning(f"Лист {sheet_name} не найден в таблице {spreadsheet_id}.")
        return

    # Получение всех значений столбца ID
    id_col_index = gspread.utils.a1_to_rowcol(f"{id_column}1")[1]
    id_values = sheet.col_values(id_col_index)

    # Поиск строки с нужным user_id
    target_row = None
    for idx, cell in enumerate(id_values, start=1):
        if re.search(r'id(\d+)', cell):
            extracted_id = re.search(r'id(\d+)', cell).group(1)
            if int(extracted_id) == target_user_id:
                target_row = idx
                break

    if not target_row:
        vk.messages.send(
            peer_id=peer_id,
            message="Пользователь не найден в таблице.",
            random_id=0
        )
        logging.info(f"Пользователь ID {target_user_id} не найден в таблице {spreadsheet_id}.")
        return

    # Получение текущих баллов
    try:
        current_points_cell = f"{points_column}{target_row}"
        current_points = sheet.acell(current_points_cell).value
        current_points = int(current_points) if current_points.isdigit() else 0
    except Exception as e:
        current_points = 0
        logging.error(f"Ошибка при получении текущих баллов: {e}")

    # Обновление баллов (уменьшение)
    new_points = current_points - points
    if new_points < 0:
        new_points = 0  # Предотвращение отрицательных баллов

    try:
        sheet.update_acell(current_points_cell, new_points)
        logging.info(f"Баллы пользователя ID {target_user_id} уменьшены с {current_points} на {points}. Новое значение: {new_points}.")
    except Exception as e:
        vk.messages.send(
            peer_id=peer_id,
            message="Не удалось обновить баллы в таблице.",
            random_id=0
        )
        logging.error(f"Ошибка при обновлении баллов: {e}")
        return

    # Отправка уведомления пользователю
    try:
        vk.messages.send(
            user_id=target_user_id,
            message=f"Вам было списано {points} баллов. Ваш текущий баланс: {new_points} баллов.",
            random_id=0
        )
        logging.info(f"Уведомление о списании баллов отправлено пользователю ID {target_user_id}.")
    except ApiError as e:
        if e.code == 901:  # Пользователь закрыл личные сообщения
            logging.warning(f"Не удалось отправить сообщение пользователю {target_user_id}: {e}")
        else:
            logging.error(f"Произошла ошибка при отправке сообщения пользователю {target_user_id}: {e}")

    # Уведомление в беседу
    vk.messages.send(
        peer_id=peer_id,
        message=f"Пользователю ID {target_user_id} списано {points} баллов.",
        random_id=0
    )

def extract_user_id(mention: str):
    # Удалим префикс http:// или https://, если он есть
    mention = re.sub(r'^https?://', '', mention)

    # Проверяем упоминание формата [id123|@nickname]
    if mention.startswith('[') and '|' in mention:
        match = re.match(r'\[(id\d+|\w+)\|@?[\w\-_]+\]', mention)
        if match:
            identifier = match.group(1)
            if identifier.startswith('id'):  # Формат [id123|@nickname]
                return int(identifier[2:])
            else:
                return get_user_id_by_screen_name(identifier)  # Формат [nickname|@nickname]

    # Если это ссылка на профиль ВК (vk.com/id12345 или vk.com/nickname)
    if mention.startswith('vk.com'):
        match = re.search(r'vk\.com/(id\d+|[\w\-_]+)', mention)
        if match:
            identifier = match.group(1)
            if identifier.startswith('id'):  # vk.com/id12345
                return int(identifier[2:])
            else:
                return get_user_id_by_screen_name(identifier)  # vk.com/nickname

    # Если упоминание начинается с @, то это может быть ID или ник
    elif mention.startswith('@'):
        mention = mention[1:]  # Удаляем символ @
        if mention.startswith('id'):  # Если формат @id12345
            return int(mention[2:])
        else:
            # Если формат @nickname, пытаемся получить ID пользователя по screen_name
            return get_user_id_by_screen_name(mention)

    # Если это просто числовой ID
    elif mention.isdigit():
        return int(mention)

    raise ValueError("Неверный формат ID. Используйте @id, @ник, ссылку или числовой ID.")
def handle_button_click(event_data):
    user_id = event_data['user_id']  # ID пользователя, который нажал кнопку
    peer_id = event_data['peer_id']  # ID беседы
    conversation_message_id = event_data['conversation_message_id']  # ID сообщения
    payload = event_data['payload']  # Данные, переданные при нажатии кнопки

    # Извлекаем информацию из payload
    action = payload.get('action')
    points = payload.get('points')
    target_user_id = payload.get('user_id')

    if action == "add_points" and points:
        points = int(points)  # Количество баллов
        cursor.execute("SELECT role FROM roles WHERE user_id = ?", (user_id,))
        role = cursor.fetchone()
        user_role = role[0] if role else 'user'

        # Проверка прав на добавление баллов
        if user_role not in ['admin', 'senmoder']:
            vk.messages.send(
                peer_id=peer_id,
                message="У вас нет прав для начисления баллов.",
                random_id=0
            )
            return

        # Обновляем баллы в Google Таблице
        add_points(peer_id, user_id, f"@id{target_user_id}", points, "Норматив", user_role)

        # Редактируем сообщение, чтобы указать, кто выдал баллы и сколько
        try:
            vk.messages.edit(
                peer_id=peer_id,
                conversation_message_id=conversation_message_id,
                message=f"✅ {points} баллов было выдано пользователю [id{target_user_id}|Пользователь].\nВыдал: [id{user_id}|Администратор]."
            )
        except ApiError as e:
            logging.error(f"Ошибка при редактировании сообщения: {e}")
def get_help_message(role):
    commands = {
        'admin': [
            "/addadmin — Назначить администратора",
            "/addsenmoder — Назначить старшего модератора",
            "/addmoder — Назначить модератора",
            "/rrole — Снять роль",
            "/bind — Привязать Google таблицу",
            "/unbind — Отвязать Google таблицу",
            "/sync — Привязать лист таблицы",
            "/sheetinfo — Информация о таблице и листе",
            "/staff — Список сотрудников",
            "/psheet — Установить столбец для баллов",
            "/stats — Просмотр статистики (по ID, нику, ссылке и reply)",
            "/warn — Выдать предупреждение",
            "/unwarn — Снять предупреждение",
            "/point — Добавить баллы",
            "/unpoint — Уменьшить баллы",
            "/wsheet — Установить столбец для предупреждений",
            "/unscr - Отвязка беседы",
            "/scr - привязка беседы для получение скринов"
            "/neaktiv - поставить неактив"
            "/nsheet - устоновить столбец для неактивов"
            "/help — Список команд"

        ],
        'senmoder': [
            "/addmoder — Назначить модератора",
            "/rrole — Снять роль",
            "/staff — Список сотрудников",
            "/psheet — Установить столбец для баллов",
            "/stats — Просмотр статистики (по ID, нику, ссылке и reply)",
            "/warn — Выдать предупреждение",
            "/unwarn — Снять предупреждение",
            "/point — Добавить баллы",
            "/unpoint — Уменьшить баллы",
            "/wsheet — Установить столбец для предупреждений",
            "/checkscr - Проверка статуса привязки"

            "/help — Список команд"
        ],
        'moder': [
            "/staff — Список сотрудников",
            "/stats — Просмотр статистики (по ID, нику, ссылке и reply)",
            "/help — Список команд"
        ],
        None: [
            "/help — Список доступных команд"
        ]
    }

    # Формируем сообщение с командами в зависимости от роли
    help_message = f"📋 *Список команд для {role if role else 'гостя'}:* \n\n"
    help_message += "\n".join(commands.get(role, commands[None]))

    return help_message

# Основной цикл для обработки событий
for event in longpoll.listen():
    if event.type == VkBotEventType.MESSAGE_NEW:
        message = event.object.message['text']
        peer_id = event.object.message['peer_id']
        from_id = event.object.message['from_id']

        # Получение роли пользователя
        user_role = get_user_role(from_id)

        # Разбор команды и аргументов
        if message.startswith('/'):
            parts = message.split()
            command = parts[0].lower()
            args = parts[1:]

            if command == '/wsheet' and user_role in ['admin', 'senmoder']:
                if len(args) != 1:
                    vk.messages.send(
                        peer_id=peer_id,
                        message="Использование: /wsheet [столбец]",
                        random_id=0
                    )
                else:
                    column = args[0]
                    set_warn_column(peer_id, column, user_role)
    if event.type == VkBotEventType.MESSAGE_NEW:
        message = event.object.message
        peer_id = message['peer_id']
        text = message['text'].strip()
        from_id = message['from_id']

        logging.info(f"Получено сообщение от пользователя {from_id} в беседе {peer_id}: {text}")

        # Проверяем роль пользователя
        user_role = get_user_role(from_id)
        logging.info(f"Роль пользователя {from_id}: {user_role}")

        # Обработка команд
        if text.startswith('/addadmin') and user_role in ['admin']:
            parts = text.split(' ', 1)
            if len(parts) == 2:
                mention = parts[1].strip()
                try:
                    user_id = extract_user_id(mention)
                    assign_role(user_role, user_id, 'admin', peer_id)
                except ValueError:
                    vk.messages.send(peer_id=peer_id, message="Неверный формат ID. Используйте @id или числовой ID.",
                                     random_id=0)
                    logging.warning(f"Неверный формат ID в команде /addadmin: {mention}")
            else:
                vk.messages.send(peer_id=peer_id, message="Укажите ID или @id для назначения роли.", random_id=0)
                logging.warning("Команда /addadmin вызвана без указания ID.")
        elif text == '/start':
            send_start_message(peer_id)
        elif text.startswith('/warn') and user_role in ['admin', 'senmoder']:
            parts = text.split(' ', 2)
            if len(parts) >= 2:
                mention = parts[1].strip()
                reason = parts[2].strip() if len(parts) >= 3 else "Причина не указана"
                warn_user(peer_id, from_id, mention, reason, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /warn @id или ID [причина]\nПример: /warn @id12345 Нарушение правил",
                    random_id=0
                )
                logging.warning("Команда /warn вызвана с некорректными параметрами.")
        elif text.startswith('/checkbinding') and user_role in ['admin']:
            handle_check_binding(peer_id)
        elif text.lower() == "/unscr":
            if user_role == 'admin':  # Только администраторы могут отвязать беседу
                handle_unbind_command(peer_id)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="❌ У вас недостаточно прав для выполнения этой команды.",
                    random_id=0
                )
        elif text.startswith('/addsenmoder') and user_role in ['admin']:
            parts = text.split(' ', 1)
            if len(parts) == 2:
                mention = parts[1].strip()
                try:
                    user_id = extract_user_id(mention)
                    assign_role(user_role, user_id, 'senmoder', peer_id)
                except ValueError:
                    vk.messages.send(peer_id=peer_id, message="Неверный формат ID. Используйте @id или числовой ID.",
                                     random_id=0)
                    logging.warning(f"Неверный формат ID в команде /addsenmoder: {mention}")
            else:
                vk.messages.send(peer_id=peer_id, message="Укажите ID или @id для назначения роли.", random_id=0)
                logging.warning("Команда /addsenmoder вызвана без указания ID.")
        elif text.startswith('/neаktiv') and user_role == 'admin':
            parts = text.split(' ', 4)
            if len(parts) == 4:
                mention = parts[1].strip()
                start_date = parts[2].strip()
                end_date = parts[3].strip()
                add_inactivity_period(peer_id, from_id, mention, start_date, end_date, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /neаktiv @id ДД.ММ.ГГГГ ДД.ММ.ГГГГ\nПример: /neаktiv @id12345 10.10.2024 12.10.2024",
                    random_id=0
                )

        elif text.startswith('/nsheet') and user_role == 'admin':
            parts = text.split(' ', 2)
            if len(parts) == 2:
                column = parts[1].strip()
                set_nektiv_column(peer_id, column, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /nsheet столбец\nПример: /nsheet E",
                    random_id=0
                )

        elif text.startswith('/addmoder') and user_role in ['admin', 'senmoder']:
            parts = text.split(' ', 1)
            if len(parts) == 2:
                mention = parts[1].strip()
                try:
                    user_id = extract_user_id(mention)
                    assign_role(user_role, user_id, 'moder', peer_id)
                except ValueError:
                    vk.messages.send(peer_id=peer_id, message="Неверный формат ID. Используйте @id или числовой ID.",
                                     random_id=0)
                    logging.warning(f"Неверный формат ID в команде /addmoder: {mention}")
            else:
                vk.messages.send(peer_id=peer_id, message="Укажите ID или @id для назначения роли.", random_id=0)
                logging.warning("Команда /addmoder вызвана без указания ID.")

        # Новая команда /rrole для снятия ролей
        elif text.startswith('/rrole') and user_role in ['admin', 'senmoder']:
            parts = text.split(' ', 1)
            if len(parts) == 2:
                mention = parts[1].strip()
                try:
                    target_user_id = extract_user_id(mention)
                    target_role = get_user_role(target_user_id)

                    if not target_role:
                        vk.messages.send(peer_id=peer_id, message="У этого пользователя нет роли.", random_id=0)
                        logging.info(f"Пользователь {target_user_id} не имеет роли для снятия.")
                        continue

                    if remove_role(user_role, target_role, peer_id):
                        delete_role(target_user_id, peer_id)
                except ValueError:
                    vk.messages.send(peer_id=peer_id, message="Неверный формат ID. Используйте @id или числовой ID.",
                                     random_id=0)
                    logging.warning(f"Неверный формат ID в команде /rrole: {mention}")
            else:
                vk.messages.send(peer_id=peer_id, message="Укажите ID или @id для снятия роли.", random_id=0)
                logging.warning("Команда /rrole вызвана без указания ID.")
        elif text.startswith('/point') and user_role in ['admin', 'senmoder']:
            parts = text.split(' ', 3)
            if len(parts) >= 3:
                mention = parts[1].strip()
                points = parts[2].strip()
                reason = parts[3].strip() if len(parts) >= 4 else "Причина не указана"
                add_points(peer_id, from_id, mention, points, reason, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /point @id количество [причина]\nПример: /point @id12345 10 За активность",
                    random_id=0
                )
                logging.warning("Команда /point вызвана с некорректными параметрами.")

        # Команды для привязки таблицы доступны только админам
        elif text.startswith('/bind') and user_role in ['admin']:
            parts = text.split(' ', 1)
            if len(parts) == 2:
                table_id = parts[1].strip()
                bind_table(peer_id, table_id)
            else:
                vk.messages.send(peer_id=peer_id, message="Пожалуйста, укажите ID таблицы после команды /bind.",
                                 random_id=0)
                logging.warning("Команда /bind вызвана без указания ID таблицы.")

        elif text.startswith('/sync') and user_role in ['admin']:
            parts = text.split(' ', 1)
            if len(parts) == 2:
                sheet_name = parts[1].strip()
                sync_sheet(peer_id, sheet_name)
            else:
                vk.messages.send(peer_id=peer_id, message="Пожалуйста, укажите название листа после команды /sync.",
                                 random_id=0)
                logging.warning("Команда /sync вызвана без указания названия листа.")

        elif text.startswith('/sheetinfo') and user_role in ['admin']:
            sheet_info(peer_id)

        elif text.startswith('/unbind') and user_role in ['admin']:
            unbind_table(peer_id)
        elif text.startswith('/help'):
            help_message = get_help_message(user_role)
            vk.messages.send(
                peer_id=peer_id,
                message=help_message,
                random_id=0
            )

        # Команда /staff для просмотра списка сотрудников доступна администраторам, старшим модераторам и модераторам
        elif text.startswith('/staff'):
            if user_role in ['admin', 'senmoder', 'moder']:
                get_staff_list(peer_id)
            else:
                vk.messages.send(peer_id=peer_id, message="У вас нет доступа к этой команде.", random_id=0)
                logging.warning(f"Пользователь {from_id} попытался использовать команду /staff без доступа.")

        # Новая команда /help для отображения доступных команд
        elif text.startswith('/menu') and user_role in ['admin']:
            parts = text.split()
            if len(parts) > 1:
                columns = parts[1:]  # все переданные параметры - это столбцы
                set_menu_ranges(peer_id, columns, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /menu <столбец 1> <столбец 2> ... <столбец N>\nПример: /menu B C D",
                    random_id=0
                )
                logging.warning("Команда /menu вызвана с некорректными параметрами.")


        elif text.startswith('/psheet') and user_role in ['admin']:
            parts = text.split()
            if len(parts) == 2:
                column = parts[1].strip().upper()
                # Если вы хотите установить новый столбец, аналогично /pointsheet:
                set_points_column(peer_id, column, user_role)
                # Если хотите отвязать столбец, используйте:
                # unset_points_column(peer_id, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /psheet <столбец>\nПример: /psheet C",
                    random_id=0
                )
                logging.warning("Команда /psheet вызвана с некорректными параметрами.")
        elif text.startswith('/repsheet') and user_role in ['admin']:
            parts = text.split()
            if len(parts) == 2:
                column = parts[1].strip().upper()
                # Установка нового столбца для ответов, аналогично /pointsheet:
                set_answers_column(peer_id, column, user_role)
                # Если хотите отвязать столбец, можете добавить функциональность:
                # unset_answers_column(peer_id, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /repsheet <столбец>\nПример: /anssheet C",
                    random_id=0
                )
                logging.warning("Команда /repsheet вызвана с некорректными параметрами.")
        elif text.startswith('/rep') and user_role in ['admin', 'senmoder']:
            parts = text.split(' ', 3)
            if len(parts) >= 3:
                mention = parts[1].strip()
                answers = parts[2].strip()
                reason = parts[3].strip() if len(parts) >= 4 else "Причина не указана"
                add_answers(peer_id, from_id, mention, answers, reason, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /rep @id количество [причина]\nПример: /ans @id12345 5 За помощь в обсуждениях",
                    random_id=0
                )
                logging.warning("Команда /rep вызвана с некорректными параметрами.")


        elif text.startswith('/unpoint') and user_role in ['admin', 'senmoder','admin']:
            parts = text.split()
            if len(parts) == 3:
                mention = parts[1].strip()
                points = parts[2].strip()
                remove_points(peer_id, from_id, mention, points, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /unpoint @id или ID <баллы>\nПример: /unpoint @id12345 5",
                    random_id=0
                )
                logging.warning("Команда /unpoint вызвана с некорректными параметрами.")

        elif text.startswith('/idsheet') and user_role in ['admin']:
            parts = text.split()
            if len(parts) == 2:
                column = parts[1].strip().upper()
                set_id_column(peer_id, column, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /idsheet <столбец>\nПример: /idsheet A",
                    random_id=0
                )
                logging.warning("Команда /idsheet вызвана с некорректными параметрами.")
        elif text.startswith('/unwarn') and user_role in ['admin', 'senmoder']:
            parts = text.split(' ', 2)
            if len(parts) >= 2:
                mention = parts[1].strip()
                reason = parts[2].strip() if len(parts) >= 3 else "Причина не указана"
                remove_warn(peer_id, from_id, mention, reason, user_role)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Использование: /unwarn @id или ID [причина]\nПример: /unwarn @id12345 Поведение улучшилось",
                    random_id=0
                )
                logging.warning("Команда /unwarn вызвана с некорректными параметрами.")

        if (text.startswith('/stats') or text == 'Статистика') and user_role in ['admin', 'senmoder', 'moder']:
            parts = text.split(maxsplit=1)
            if len(parts) == 1:
                # Команда /stats для текущего пользователя
                get_stats(peer_id, from_id, user_role, user_role)
            elif len(parts) == 2:
                # Команда /stats @id
                if user_role in ['admin', 'senmoder']:
                    mention = parts[1].strip()
                    try:
                        target_user_id = extract_user_id(mention)
                        get_stats(peer_id, target_user_id, user_role, user_role)
                    except ValueError:
                        vk.messages.send(peer_id=peer_id,
                                         message="Неверный формат ID. Используйте @id или числовой ID.", random_id=0)
                        logging.warning(f"Неверный формат ID в команде /stats: {mention}")
                else:
                    vk.messages.send(peer_id=peer_id,
                                     message="У вас нет прав для просмотра статистики других пользователей.",
                                     random_id=0)
                    logging.warning(f"Пользователь {from_id} попытался использовать команду /stats @id без прав.")
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="Неверный формат команды. Используйте /stats или /stats @id.",
                    random_id=0
                )
                logging.warning(f"Команда /stats вызвана с некорректными параметрами: {text}")
    if event.type == VkBotEventType.MESSAGE_NEW:
        message = event.object.message
        peer_id = message['peer_id']
        from_id = message['from_id']
        text = message['text'].strip()

        logging.info(f"Получено сообщение от пользователя {from_id} в беседе {peer_id}: {text}")

        # Получение роли пользователя
        cursor.execute("SELECT role FROM roles WHERE user_id = ?", (from_id,))
        role = cursor.fetchone()
        user_role = role[0] if role else 'user'
        logging.info(f"Роль пользователя {from_id}: {user_role}")
        if text.lower() == "/scr":
            # Проверка роли пользователя
            if user_role == 'admin':  # Только администраторы могут привязать беседу
                handle_getscreen_command(peer_id, from_id)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="❌ У вас недостаточно прав для выполнения этой команды.",
                    random_id=0
                )
        elif text.lower() == "/unscr" and user_role in ['admin', 'senmoder']:
            # Отвязка беседы (только администраторы)
            if user_role == 'admin':
                handle_unbindscreen_command(peer_id)
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="❌ У вас недостаточно прав для выполнения этой команды.",
                    random_id=0
                )

        elif text.lower() == "/checkscr" and user_role in ['admin', 'senmoder']:
            # Проверка статуса привязки (для всех пользователей)
            handle_checkscreen_command(peer_id)

        # Если команда /norma
        if text.lower() == "/norma" and user_role in ['admin', 'senmoder', 'moder']:
            if user_role in ['admin', 'senmoder', 'moder']:
                vk.messages.send(
                    peer_id=peer_id,
                    message="📷 Пришлите скриншот норматива.",
                    random_id=0
        )
            else:
                vk.messages.send(
                    peer_id=peer_id,
                    message="❌ У вас нет роли для отправки норматива.",
            random_id=0
        )
        elif 'attachments' in message and len(message['attachments']) > 0:
            # Пересылаем сообщение с фото администратору
            handle_norma(peer_id, from_id, message['id'])
        elif event.type == VkBotEventType.MESSAGE_EVENT:
            event_data = event.object
            handle_button_click(event_data)


        # Обработка нажатий на инлайн-кнопки
    elif event.type == VkBotEventType.MESSAGE_EVENT:
        event_data = event.object
        payload = event_data.get('payload', {})
        action_data = payload

        if action_data.get('action') == 'add_points':
            points = int(action_data.get('points'))
        target_user_id = action_data.get('user_id')
        performer_id = event_data.get('user_id')  # ID администратора, который нажал кнопку
        peer_id = event_data.get('peer_id')
        conversation_message_id = event_data.get('conversation_message_id')  # ID сообщения в беседе
        screenshot_number = action_data.get('screenshot_number')  # Получаем фиксированный номер скриншота

        mention = f"https://vk.com/id{target_user_id}"
        reason = "Начисление за выполнение норматива"
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Текущая дата и время

        # Вызов функции add_points
        user_role = "admin"  # Предположим, что пользователь имеет роль admin для этого примера

        # Получение текущего баланса до начисления баллов
        current_points = get_current_balance(target_user_id)

        # Локально рассчитываем новый баланс
        new_points = current_points + points

        # Редактирование исходного сообщения с расчетным новым балансом
        vk.messages.edit(
            peer_id=peer_id,
            message=f"#{screenshot_number} 💎 [{mention}|Пользователь] получил {points} баллов!*\n"
                    f"*Начислено администратором: [id{performer_id}|администратор]*\n"
                    f"*Причина начисления: {reason}*\n"
                    f"Дата начисления: {current_time}\n"
                    f"Текущий баланс: {new_points} баллов",
            conversation_message_id=conversation_message_id
        )

        # Обновляем баллы в Google Таблицах
        add_points(peer_id, performer_id, mention, points, reason, user_role, send_notification=False)

