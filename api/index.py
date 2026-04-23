from http.server import BaseHTTPRequestHandler
import json
import os
import requests
import traceback
from datetime import datetime, timedelta
import uuid

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

# === FIRESTORE REST API ===
FIRESTORE_URL = "https://firestore.googleapis.com/v1/projects/grafikpro-d3500/databases/(default)/documents"
API_KEY = "AIzaSyAmP4IW-mcqhXT1L6s4vx5_Z7IZbi1YqI8"

def firestore_get(collection, doc_id):
    url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()
        fields = data.get("fields", {})
        result = {}
        for key, value in fields.items():
            if "stringValue" in value: result[key] = value["stringValue"]
            elif "arrayValue" in value:
                vals = value["arrayValue"].get("values", [])
                result[key] = [v.get("stringValue", str(v.get("integerValue", ""))) for v in vals] if vals else []
            elif "integerValue" in value: result[key] = int(value["integerValue"])
        return result
    return None

def firestore_set(collection, doc_id, data):
    url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
    fields = {}
    for key, val in data.items():
        if isinstance(val, str): fields[key] = {"stringValue": val}
        elif isinstance(val, list): fields[key] = {"arrayValue": {"values": [{"stringValue": str(item)} for item in val]}}
        elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
    return requests.patch(url, json={"fields": fields}).status_code in [200, 201]

def firestore_query(collection, field, operator, value):
    url = f"{FIRESTORE_URL}:runQuery?key={API_KEY}"
    body = {"structuredQuery": {"from": [{"collectionId": collection}], "where": {"fieldFilter": {"field": {"fieldPath": field}, "op": operator, "value": {"stringValue": str(value)}}}}}
    r = requests.post(url, json=body)
    results = []
    if r.status_code == 200:
        for doc in r.json():
            if "document" in doc:
                doc_data = {}
                fields = doc["document"].get("fields", {})
                for key, val in fields.items():
                    if "stringValue" in val: doc_data[key] = val["stringValue"]
                    elif "arrayValue" in val: doc_data[key] = [v.get("stringValue", "") for v in val.get("values", [])]
                doc_data["_id"] = doc["document"]["name"].split("/")[-1]
                results.append(doc_data)
    return results

def firestore_add(collection, data):
    url = f"{FIRESTORE_URL}/{collection}?key={API_KEY}"
    fields = {}
    for key, val in data.items():
        if isinstance(val, str): fields[key] = {"stringValue": val}
        elif isinstance(val, list): fields[key] = {"arrayValue": {"values": [{"stringValue": str(item)} for item in val]}}
        elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
    return requests.post(url, json={"fields": fields}).status_code in [200, 201]

# === Telegram API ===
def send_message(chat_id, text, reply_markup=None, parse_mode=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
    if parse_mode: payload["parse_mode"] = parse_mode
    requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload)

# === Клавиатуры ===
def master_menu():
    return {"keyboard": [["📅 Моё расписание", "➕ Новая запись"], ["👥 Клиенты", "📊 Статистика"], ["🔗 Моя ссылка", "⚙️ Настройки"]], "resize_keyboard": True}

def settings_menu():
    return {"keyboard": [["💈 Мои услуги", "⏰ Рабочие часы"], ["🔙 Назад в меню"]], "resize_keyboard": True}

def services_inline(services):
    buttons = [[{"text": f"❌ {s}", "callback_data": f"del_service_{s}"}] for s in services]
    buttons.append([{"text": "➕ Добавить услугу", "callback_data": "add_service"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "back_to_settings"}])
    return {"inline_keyboard": buttons}

STATES = {}

# === Основа ===
def handle_start(chat_id, user_name):
    master = firestore_get("masters", str(chat_id))
    if master:
        send_message(chat_id, f"С возвращением, мастер {user_name}!", reply_markup=master_menu())
    else:
        send_message(chat_id, "👋 Добро пожаловать в *График.Про*!\n\nКто вы?", reply_markup={"keyboard": [["👤 Я мастер", "👥 Я клиент"]], "resize_keyboard": True})

def handle_master_registration(chat_id, user_name, username):
    if not firestore_get("masters", str(chat_id)):
        firestore_set("masters", str(chat_id), {"name": user_name, "username": username, "services": [], "schedule": {"start": "09:00", "end": "18:00"}, "created_at": datetime.now().isoformat()})
    send_message(chat_id, "✅ Вы зарегистрированы как мастер!", reply_markup=master_menu())

def handle_master_link(chat_id):
    if not firestore_get("masters", str(chat_id)):
        return send_message(chat_id, "Сначала зарегистрируйтесь как мастер.")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": str(chat_id)})
    send_message(chat_id, f"🔗 *Ваша ссылка:*\n\n`https://t.me/grafikpro_bot?start=master_{link_id}`", parse_mode="Markdown")

def handle_schedule_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return
    schedule = master.get("schedule", {"start": "09:00", "end": "18:00"})
    send_message(chat_id, f"⏰ *Рабочие часы*\n\nСейчас: с {schedule.get('start', '09:00')} до {schedule.get('end', '18:00')}\n\nЧтобы изменить, отправьте время в формате:\n`09:00-20:00`", parse_mode="Markdown")
    STATES[str(chat_id)] = {"state": "setting_schedule"}

def handle_schedule_set(chat_id, text):
    try:
        start, end = text.split("-")
        # Простая валидация
        if ":" in start and ":" in end:
            firestore_set("masters", str(chat_id), {"schedule": {"start": start.strip(), "end": end.strip()}})
            send_message(chat_id, f"✅ Рабочие часы обновлены: с {start} до {end}", reply_markup=settings_menu())
        else:
            send_message(chat_id, "❌ Неверный формат. Попробуйте ещё раз: 09:00-20:00")
            return
    except:
        send_message(chat_id, "❌ Ошибка формата. Отправьте: 09:00-20:00")
        return
    STATES.pop(str(chat_id), None)

def handle_schedule_view(chat_id):
    master_id = str(chat_id)
    appointments = firestore_query("appointments", "master_id", "EQUAL", master_id)
    if not appointments:
        return send_message(chat_id, "📭 На этой неделе записей пока нет.")
    
    text = "📅 *Моё расписание:*\n"
    for appt in appointments:
        text += f"\n• {appt.get('date', '?')} в {appt.get('time', '?')} — {appt.get('service', '?')} (Клиент ID: {appt.get('client_id', '?')[:4]}...)"
    send_message(chat_id, text)

def handle_client_start_from_link(chat_id, link_id):
    link = firestore_get("links", link_id)
    if not link: return send_message(chat_id, "❌ Ссылка недействительна.")
    master = firestore_get("masters", link["master_id"])
    if not master: return send_message(chat_id, "❌ Мастер не найден.")
    services = master.get("services", [])
    if not services: return send_message(chat_id, "❌ У мастера пока нет услуг.")
    buttons = [[{"text": s, "callback_data": f"client_service_{link_id}_{s}"}] for s in services]
    send_message(chat_id, f"📝 *Запись к {master.get('name')}*\nВыберите услугу:", reply_markup={"inline_keyboard": buttons})

def handle_client_service_select(chat_id, link_id, service_name):
    link = firestore_get("links", link_id)
    master = firestore_get(link["master_id"])
    schedule = master.get("schedule", {"start": "09:00", "end": "18:00"})
    start_h = int(schedule["start"].split(":")[0])
    end_h = int(schedule["end"].split(":")[0])
    
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    slots = [f"{h}:00" for h in range(start_h, end_h + 1)]
    buttons = [[{"text": t, "callback_data": f"client_time_{link_id}_{service_name}_{tomorrow}_{t}"}] for t in slots]
    send_message(chat_id, f"📅 *Выберите время:*\n{service_name} | {tomorrow}", reply_markup={"inline_keyboard": buttons})

def handle_client_time_select(chat_id, link_id, service_name, date, time):
    link = firestore_get("links", link_id)
    master_id, master_name = link["master_id"], firestore_get(link["master_id"]).get("name", "Мастер")
    firestore_add("appointments", {"master_id": master_id, "client_id": str(chat_id), "service": service_name, "date": date, "time": time, "status": "confirmed"})
    send_message(chat_id, f"✅ *Запись подтверждена!*\n\n{master_name}\n{service_name}\n{date} в {time}")
    send_message(int(master_id), f"🔔 *Новая запись!*\n\n{service_name}\n{date} в {time}")

# === Сервисы и настройки ===
def handle_settings_services(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return
    services = master.get("services", [])
    text = "💈 *Ваши услуги:*" if services else "💈 Услуг пока нет."
    send_message(chat_id, text, reply_markup=services_inline(services))

def handle_add_service_prompt(chat_id):
    STATES[str(chat_id)] = {"state": "adding_service"}
    send_message(chat_id, "✏️ Введите название услуги:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_add_service_name(chat_id, service_name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = master.get("services", [])
        services.append(service_name)
        firestore_set("masters", str(chat_id), {"services": services})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ *«{service_name}»* добавлена!", reply_markup=settings_menu())
    handle_settings_services(chat_id)

def handle_delete_service(chat_id, service_name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = [s for s in master.get("services", []) if s != service_name]
        firestore_set("masters", str(chat_id), {"services": services})
    handle_settings_services(chat_id)

# === Маршрутизация текста ===
def handle_text(chat_id, user_name, username, text):
    state = STATES.get(str(chat_id), {}).get("state")
    if state == "adding_service":
        if text == "🔙 Отмена":
            STATES.pop(str(chat_id), None)
            return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_name(chat_id, text)
    if state == "setting_schedule":
        return handle_schedule_set(chat_id, text)

    if text == "👤 Я мастер": handle_master_registration(chat_id, user_name, username)
    elif text == "⚙️ Настройки": send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Мои услуги": handle_settings_services(chat_id)
    elif text == "⏰ Рабочие часы": handle_schedule_settings(chat_id)
    elif text == "🔙 Назад в меню": send_message(chat_id, "Главное меню", reply_markup=master_menu())
    elif text == "🔗 Моя ссылка": handle_master_link(chat_id)
    elif text == "📅 Моё расписание": handle_schedule_view(chat_id)
    else: send_message(chat_id, "Используйте меню.", reply_markup=master_menu())

def handle_callback(chat_id, data):
    if data == "add_service": handle_add_service_prompt(chat_id)
    elif data.startswith("del_service_"): handle_delete_service(chat_id, data.replace("del_service_", "", 1))
    elif data == "back_to_settings": send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data.startswith("client_service_"):
        _, link_id, service_name = data.split("_", 2)
        handle_client_service_select(chat_id, link_id, service_name)
    elif data.startswith("client_time_"):
        _, link_id, service_name, date, time = data.split("_", 4)
        handle_client_time_select(chat_id, link_id, service_name, date, time)

def process_update(update):
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        user_name = msg["from"].get("first_name", "Пользователь")
        if "text" in msg:
            text = msg["text"]
            if text.startswith("/start"):
                if "master_" in text: handle_client_start_from_link(chat_id, text.split("master_")[1])
                else: handle_start(chat_id, user_name)
            else: handle_text(chat_id, user_name, msg["from"].get("username", ""), text)
    elif "callback_query" in update:
        handle_callback(update["callback_query"]["message"]["chat"]["id"], update["callback_query"]["data"])

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length:
            try: process_update(json.loads(self.rfile.read(content_length).decode('utf-8')))
            except Exception as e: print(f"Error: {e}\n{traceback.format_exc()}")
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "bot online"}).encode())