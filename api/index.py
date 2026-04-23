from http.server import BaseHTTPRequestHandler
import json
import os
import requests
import traceback

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

# === БАЗА ДАННЫХ (бесплатный JSON на Vercel) ===
DB_FILE = "/tmp/database.json"

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"masters": {}, "appointments": []}

def save_db(data):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# === API Telegram ===
def send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    if parse_mode:
        payload["parse_mode"] = parse_mode
    requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload)

# === КЛАВИАТУРЫ ===
def master_menu():
    return {
        "keyboard": [
            ["📅 Моё расписание", "➕ Новая запись"],
            ["👥 Клиенты", "📊 Статистика"]
        ],
        "resize_keyboard": True
    }

def client_menu():
    return {
        "keyboard": [
            ["🔍 Найти мастера", "📋 Мои записи"]
        ],
        "resize_keyboard": True
    }

# === ОБРАБОТЧИКИ СООБЩЕНИЙ ===
def handle_start(chat_id, user_name):
    db = load_db()
    if str(chat_id) in db["masters"]:
        send_message(chat_id, f"С возвращением, мастер {user_name}!", reply_markup=master_menu())
    else:
        keyboard = {
            "keyboard": [["👤 Я мастер", "👥 Я клиент"]],
            "resize_keyboard": True
        }
        send_message(chat_id, "👋 Добро пожаловать в *График.Про*!\n\nЯ помогу записывать клиентов и не терять деньги.\n\n*Кто вы?*", reply_markup=keyboard)

def handle_master_registration(chat_id, user_name, username):
    db = load_db()
    db["masters"][str(chat_id)] = {
        "name": user_name,
        "username": username,
        "services": ["Стрижка", "Укладка"],
        "registered_at": "now"
    }
    save_db(db)
    send_message(chat_id, "✅ Отлично! Вы зарегистрированы как мастер.\n\n📅 Теперь вы можете управлять записями через кнопки меню.", reply_markup=master_menu())

def handle_text(chat_id, user_name, username, text):
    db = load_db()
    
    if text == "👤 Я мастер":
        if str(chat_id) in db["masters"]:
            send_message(chat_id, "Вы уже зарегистрированы!", reply_markup=master_menu())
        else:
            handle_master_registration(chat_id, user_name, username)
    
    elif text == "👥 Я клиент":
        send_message(chat_id, "🔍 Раздел клиента в разработке.\nНапишите /start для возврата.", reply_markup=client_menu())
    
    elif text == "📅 Моё расписание":
        send_message(chat_id, "📭 На этой неделе записей пока нет.")
    
    elif text == "➕ Новая запись":
        send_message(chat_id, "📝 Чтобы добавить запись, попросите клиента написать боту и выбрать вас как мастера. Эта функция появится в следующем обновлении!")
    
    elif text == "👥 Клиенты":
        send_message(chat_id, "👥 Список клиентов пока пуст. Как только появятся записи, они отобразятся здесь.")
    
    elif text == "📊 Статистика":
        send_message(chat_id, "📊 Здесь будет доход, загрузка и прогноз на неделю. Собираем данные!")
    
    else:
        send_message(chat_id, "Используйте кнопки меню для навигации.")

def process_update(update):
    if "message" in update:
        message = update["message"]
        chat_id = message["chat"]["id"]
        user_name = message["from"].get("first_name", "Мастер")
        username = message["from"].get("username", "")
        
        if "text" in message:
            text = message["text"]
            if text.startswith("/start"):
                handle_start(chat_id, user_name)
            else:
                handle_text(chat_id, user_name, username, text)

# === СЕРВЕР ===
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length else b''
        try:
            update = json.loads(post_data.decode('utf-8'))
            process_update(update)
        except Exception as e:
            print(f"Error: {e}\n{traceback.format_exc()}")
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "bot online"}).encode())