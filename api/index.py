from http.server import BaseHTTPRequestHandler
import json
import requests
import traceback
from datetime import datetime, timedelta
import uuid
import re

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"
FIRESTORE_URL = "https://firestore.googleapis.com/v1/projects/grafikpro-d3500/databases/(default)/documents"
API_KEY = "AIzaSyAmP4IW-mcqhXT1L6s4vx5_Z7IZbi1YqI8"

# === БАЗА ДАННЫХ (исправлено чтение mapValue в массивах) ===
def firestore_get(collection, doc_id):
    r = requests.get(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}")
    if r.status_code != 200: return None
    fields = r.json().get("fields", {})
    result = {}
    for key, value in fields.items():
        if "stringValue" in value:
            result[key] = value["stringValue"]
        elif "arrayValue" in value:
            arr = []
            for v in value["arrayValue"].get("values", []):
                if "stringValue" in v:
                    arr.append(v["stringValue"])
                elif "mapValue" in v:
                    inner = v["mapValue"].get("fields", {})
                    if inner:  # игнорируем пустые {}
                        item = {}
                        for k, iv in inner.items():
                            if "stringValue" in iv: item[k] = iv["stringValue"]
                            elif "integerValue" in iv: item[k] = int(iv["integerValue"])
                        arr.append(item)
            result[key] = arr
        elif "mapValue" in value:
            inner = {}
            for k, iv in value["mapValue"].get("fields", {}).items():
                if "stringValue" in iv: inner[k] = iv["stringValue"]
                elif "integerValue" in iv: inner[k] = int(iv["integerValue"])
            result[key] = inner
        elif "integerValue" in value:
            result[key] = int(value["integerValue"])
    return result

def firestore_set(collection, doc_id, data):
    update_fields = list(data.keys())
    url = f"{FIRESTORE_URL}/{collection}/{doc_id}?updateMask.fieldPaths={'&updateMask.fieldPaths='.join(update_fields)}&key={API_KEY}"
    fields = {}
    for key, val in data.items():
        if isinstance(val, str): fields[key] = {"stringValue": val}
        elif isinstance(val, list):
            items = []
            for item in val:
                if isinstance(item, dict):
                    map_fields = {}
                    for k, v in item.items():
                        if isinstance(v, str): map_fields[k] = {"stringValue": v}
                        elif isinstance(v, int): map_fields[k] = {"integerValue": str(v)}
                    items.append({"mapValue": {"fields": map_fields}})
                elif isinstance(item, str):
                    items.append({"stringValue": item})
            fields[key] = {"arrayValue": {"values": items}}
        elif isinstance(val, dict):
            map_fields = {}
            for k, v in val.items():
                if isinstance(v, str): map_fields[k] = {"stringValue": v}
                elif isinstance(v, int): map_fields[k] = {"integerValue": str(v)}
            fields[key] = {"mapValue": {"fields": map_fields}}
        elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
    return requests.patch(url, json={"fields": fields}).status_code in [200, 201]

def firestore_query(collection, field, operator, value):
    body = {"structuredQuery": {"from": [{"collectionId": collection}], "where": {"fieldFilter": {"field": {"fieldPath": field}, "op": operator, "value": {"stringValue": str(value)}}}}}
    r = requests.post(f"{FIRESTORE_URL}:runQuery?key={API_KEY}", json=body)
    results = []
    if r.status_code == 200:
        for doc in r.json():
            if "document" in doc:
                fields = doc["document"].get("fields", {})
                data = {}
                for key, val in fields.items():
                    if "stringValue" in val: data[key] = val["stringValue"]
                    elif "arrayValue" in val:
                        arr = []
                        for v in val["arrayValue"].get("values", []):
                            if "stringValue" in v: arr.append(v["stringValue"])
                            elif "mapValue" in v:
                                inner = v["mapValue"].get("fields", {})
                                if inner:
                                    item = {}
                                    for k, iv in inner.items():
                                        if "stringValue" in iv: item[k] = iv["stringValue"]
                                        elif "integerValue" in iv: item[k] = int(iv["integerValue"])
                                    arr.append(item)
                        data[key] = arr
                    elif "integerValue" in val: data[key] = int(val["integerValue"])
                data["_id"] = doc["document"]["name"].split("/")[-1]
                results.append(data)
    return results

def firestore_add(collection, data):
    fields = {}
    for key, val in data.items():
        if isinstance(val, str): fields[key] = {"stringValue": val}
        elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
    return requests.post(f"{FIRESTORE_URL}/{collection}?key={API_KEY}", json={"fields": fields}).status_code in [200, 201]

# === TELEGRAM ===
def send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
    if parse_mode: payload["parse_mode"] = parse_mode
    requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload)

def master_menu():
    return {"keyboard": [["📅 Расписание", "➕ Мои клиенты"], ["📊 Статистика", "🔗 Моя ссылка"], ["⚙️ Настройки"]], "resize_keyboard": True}

def settings_menu():
    return {"keyboard": [["💈 Услуги", "⏰ Часы"], ["🔙 В меню"]], "resize_keyboard": True}

def services_list_keyboard(services):
    buttons = []
    for s in services:
        if isinstance(s, dict) and s:  # непустой словарь
            name = s.get("name", "?")
            price = s.get("price", 0)
            buttons.append([{"text": f"❌ {name} ({price}₽)", "callback_data": f"delservice_{name}"}])
    buttons.append([{"text": "➕ Добавить", "callback_data": "addservice"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    return {"inline_keyboard": buttons}

STATES = {}

# === СТАРТ ===
def handle_start(chat_id, user_name):
    if firestore_get("masters", str(chat_id)):
        send_message(chat_id, f"С возвращением, {user_name}!", reply_markup=master_menu())
    else:
        send_message(chat_id, "👋 Добро пожаловать в *График.Про*!\n\nКто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})

def handle_master_registration(chat_id, user_name, username):
    if not firestore_get("masters", str(chat_id)):
        firestore_set("masters", str(chat_id), {
            "name": user_name, "username": username, "services": [],
            "schedule": {"start": "09:00", "end": "18:00"},
            "created_at": datetime.now().isoformat()
        })
    send_message(chat_id, "✅ Вы зарегистрированы!", reply_markup=master_menu())

def handle_master_link(chat_id):
    if not firestore_get("masters", str(chat_id)):
        return send_message(chat_id, "Сначала зарегистрируйтесь.")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": str(chat_id)})
    send_message(chat_id, f"🔗 *Ваша ссылка:*\n\n`https://t.me/grafikpro_bot?start=master_{link_id}`", parse_mode="Markdown")

# === УСЛУГИ ===
def handle_services_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    services = master.get("services", [])
    # Фильтруем только словари с именем
    valid = [s for s in services if isinstance(s, dict) and s.get("name")]
    text = "💈 *Услуги:*" if valid else "💈 Нет услуг."
    send_message(chat_id, text, reply_markup=services_list_keyboard(valid))

def handle_add_service_start(chat_id):
    STATES[str(chat_id)] = {"state": "adding_service_name"}
    send_message(chat_id, "✏️ Название услуги:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_add_service_name(chat_id, name):
    STATES[str(chat_id)] = {"state": "adding_service_price", "name": name}
    send_message(chat_id, f"💰 Цена для «{name}»:")

def handle_add_service_price(chat_id, price_text):
    state = STATES.get(str(chat_id), {})
    name = state.get("name", "")
    try:
        price = int(price_text.strip())
    except:
        return send_message(chat_id, "❌ Введите число.")
    
    master = firestore_get("masters", str(chat_id))
    services = master.get("services", [])
    # Убираем мусор (пустые словари)
    services = [s for s in services if isinstance(s, dict) and s.get("name")]
    services.append({"name": name, "price": price})
    firestore_set("masters", str(chat_id), {"services": services})
    
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ *{name}* — {price}₽", reply_markup=settings_menu())

def handle_delete_service(chat_id, name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = master.get("services", [])
        services = [s for s in services if isinstance(s, dict) and s.get("name") != name]
        firestore_set("masters", str(chat_id), {"services": services})
    handle_services_settings(chat_id)

# === РАСПИСАНИЕ ===
def handle_schedule_view(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "📭 Записей пока нет.")
    appointments.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    today = datetime.now().strftime('%Y-%m-%d')
    text = "📅 *Расписание:*\n"
    for a in appointments:
        date_str = "Сегодня" if a.get("date") == today else a.get("date", "?")
        text += f"\n• {date_str} {a.get('time')} — {a.get('service')}\n  {a.get('client_name', 'Клиент')} | {a.get('client_phone', '')}"
    send_message(chat_id, text, reply_markup=master_menu())

# === ЧАСЫ ===
def handle_schedule_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    sched = master.get("schedule", {"start": "09:00", "end": "18:00"})
    STATES[str(chat_id)] = {"state": "setting_schedule"}
    send_message(chat_id, f"⏰ *Часы работы*\nСейчас: {sched.get('start')} – {sched.get('end')}\n\nОтправьте: `09:00-20:00`", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_schedule_set(chat_id, text):
    try:
        start, end = text.strip().split("-")
        firestore_set("masters", str(chat_id), {"schedule": {"start": start.strip(), "end": end.strip()}})
        STATES.pop(str(chat_id), None)
        send_message(chat_id, f"✅ {start} – {end}", reply_markup=settings_menu())
    except:
        send_message(chat_id, "❌ Формат: 09:00-20:00")

# === КЛИЕНТ ===
def handle_client_start(chat_id, link_id):
    link = firestore_get("links", link_id)
    if not link: return send_message(chat_id, "❌ Ссылка недействительна.")
    master = firestore_get("masters", link["master_id"])
    if not master: return send_message(chat_id, "❌ Мастер не найден.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not services: return send_message(chat_id, "❌ Нет услуг.")
    
    buttons = [[{"text": f"{s['name']} — {s['price']}₽", "callback_data": f"cl_srv_{link_id}_{s['name']}"}] for s in services]
    STATES[str(chat_id)] = {"link_id": link_id, "master_name": master.get("name")}
    send_message(chat_id, f"📝 *Запись к {master.get('name')}*\nВыберите услугу:", reply_markup={"inline_keyboard": buttons})

def handle_client_service_select(chat_id, link_id, service_name):
    STATES[str(chat_id)] = STATES.get(str(chat_id), {})
    STATES[str(chat_id)]["service"] = service_name
    STATES[str(chat_id)]["state"] = "client_picking_date"
    
    buttons = []
    for i in range(7):
        date = (datetime.now() + timedelta(days=i+1)).strftime('%Y-%m-%d')
        label = (datetime.now() + timedelta(days=i+1)).strftime('%d.%m (%a)')
        buttons.append([{"text": label, "callback_data": f"cl_date_{link_id}_{date}"}])
    
    send_message(chat_id, f"📅 *{service_name}*\nВыберите дату:", reply_markup={"inline_keyboard": buttons})

def handle_client_date_select(chat_id, link_id, date):
    master = firestore_get(firestore_get("links", link_id)["master_id"])
    sched = master.get("schedule", {"start": "09:00", "end": "18:00"})
    start_h = int(sched["start"].split(":")[0])
    end_h = int(sched["end"].split(":")[0])
    
    all_appts = firestore_query("appointments", "master_id", "EQUAL", firestore_get("links", link_id)["master_id"])
    busy = {a.get("time") for a in all_appts if a.get("date") == date}
    
    buttons = []
    for h in range(start_h, end_h + 1):
        slot = f"{h}:00"
        if slot in busy:
            buttons.append([{"text": f"❌ {slot} (занято)", "callback_data": "ignore"}])
        else:
            buttons.append([{"text": f"🟢 {slot}", "callback_data": f"cl_time_{link_id}_{date}_{slot}"}])
    
    STATES[str(chat_id)]["date"] = date
    send_message(chat_id, f"⏰ *Выберите время:*\n{date}", reply_markup={"inline_keyboard": buttons})

def handle_client_time_select(chat_id, link_id, date, time):
    STATES[str(chat_id)] = STATES.get(str(chat_id), {})
    STATES[str(chat_id)]["time"] = time
    STATES[str(chat_id)]["state"] = "client_entering_name"
    send_message(chat_id, "📝 *Ваше имя:*", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_client_name(chat_id, name):
    STATES[str(chat_id)]["client_name"] = name
    STATES[str(chat_id)]["state"] = "client_entering_phone"
    send_message(chat_id, "📞 *Ваш телефон:* (+79001234567)")

def handle_client_phone(chat_id, phone):
    state = STATES.get(str(chat_id), {})
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    if len(phone_clean) < 10:
        return send_message(chat_id, "❌ Неверный формат.")
    
    link = firestore_get("links", state.get("link_id", ""))
    master_id = link["master_id"]
    master = firestore_get(master_id)
    
    firestore_add("appointments", {
        "master_id": master_id,
        "client_id": str(chat_id),
        "client_name": state.get("client_name", ""),
        "client_phone": phone_clean,
        "service": state.get("service", ""),
        "date": state.get("date", ""),
        "time": state.get("time", ""),
        "status": "confirmed"
    })
    
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ *Запись подтверждена!*\n\n{master.get('name')}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}\n{state.get('client_name')} | {phone_clean}")
    send_message(int(master_id), f"🔔 *Новая запись!*\n\n{state.get('client_name')}\n{phone_clean}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}")

# === ОБРАБОТКА ===
def handle_text(chat_id, user_name, username, text):
    state = STATES.get(str(chat_id), {}).get("state")
    
    if state == "adding_service_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_name(chat_id, text)
    if state == "adding_service_price":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_price(chat_id, text)
    if state == "client_entering_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.")
        return handle_client_name(chat_id, text)
    if state == "client_entering_phone":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.")
        return handle_client_phone(chat_id, text)
    if state == "setting_schedule":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_schedule_set(chat_id, text)
    
    if text == "👤 Я мастер": handle_master_registration(chat_id, user_name, username)
    elif text == "👥 Я клиент": send_message(chat_id, "Используйте ссылку мастера.")
    elif text == "⚙️ Настройки": send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Услуги": handle_services_settings(chat_id)
    elif text == "⏰ Часы": handle_schedule_settings(chat_id)
    elif text == "🔙 В меню": STATES.pop(str(chat_id), None); send_message(chat_id, "Главное меню", reply_markup=master_menu())
    elif text == "🔗 Моя ссылка": handle_master_link(chat_id)
    elif text == "📅 Расписание": handle_schedule_view(chat_id)
    elif text == "➕ Мои клиенты": send_message(chat_id, "👥 После первых записей.")
    elif text == "📊 Статистика": send_message(chat_id, "📊 Ждём данных.")
    else: send_message(chat_id, "Используйте меню.", reply_markup=master_menu())

def handle_callback(chat_id, data):
    if data == "addservice": handle_add_service_start(chat_id)
    elif data.startswith("delservice_"): handle_delete_service(chat_id, data.replace("delservice_", "", 1))
    elif data == "settings_back": send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data.startswith("cl_srv_"):
        _, link_id, service = data.split("_", 2)
        handle_client_service_select(chat_id, link_id, service)
    elif data.startswith("cl_date_"):
        _, link_id, date = data.split("_", 2)
        handle_client_date_select(chat_id, link_id, date)
    elif data.startswith("cl_time_"):
        _, link_id, date, time = data.split("_", 3)
        handle_client_time_select(chat_id, link_id, date, time)
    elif data == "ignore": pass

def process_update(update):
    if "message" in update:
        msg = update["message"]
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "")
        user_name = msg["from"].get("first_name", "Пользователь")
        if text.startswith("/start"):
            if "master_" in text: handle_client_start(chat_id, text.split("master_")[1])
            else: handle_start(chat_id, user_name)
        else: handle_text(chat_id, user_name, msg["from"].get("username", ""), text)
    elif "callback_query" in update:
        handle_callback(update["callback_query"]["message"]["chat"]["id"], update["callback_query"]["data"])

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        cl = int(self.headers.get('Content-Length', 0))
        if cl:
            try: process_update(json.loads(self.rfile.read(cl).decode('utf-8')))
            except Exception as e: print(f"Error: {e}\n{traceback.format_exc()}")
        self.send_response(200); self.send_header('Content-type','application/json'); self.end_headers()
        self.wfile.write(json.dumps({"status":"ok"}).encode())
    def do_GET(self):
        self.send_response(200); self.send_header('Content-type','application/json'); self.end_headers()
        self.wfile.write(json.dumps({"status":"bot online"}).encode())