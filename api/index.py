from http.server import BaseHTTPRequestHandler
import json
import requests
import traceback
from datetime import datetime, timedelta
import uuid
import re
import threading
import time

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"
FIRESTORE_URL = "https://firestore.googleapis.com/v1/projects/grafikpro-d3500/databases/(default)/documents"
API_KEY = "AIzaSyAmP4IW-mcqhXT1L6s4vx5_Z7IZbi1YqI8"

def firestore_get(collection, doc_id):
    r = requests.get(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}")
    if r.status_code != 200: return None
    fields = r.json().get("fields", {})
    result = {}
    for key, value in fields.items():
        if "stringValue" in value: result[key] = value["stringValue"]
        elif "arrayValue" in value:
            arr = []
            for v in value["arrayValue"].get("values", []):
                if "stringValue" in v: arr.append(v["stringValue"])
                elif "mapValue" in v:
                    inner = v["mapValue"].get("fields", {})
                    if inner:
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
        elif "integerValue" in value: result[key] = int(value["integerValue"])
    return result

def firestore_set(collection, doc_id, data):
    url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
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
                elif isinstance(item, str): items.append({"stringValue": item})
            fields[key] = {"arrayValue": {"values": items}}
        elif isinstance(val, dict):
            map_fields = {}
            for k, v in val.items():
                if isinstance(v, str): map_fields[k] = {"stringValue": v}
                elif isinstance(v, int): map_fields[k] = {"integerValue": str(v)}
                elif isinstance(v, dict):
                    inner = {}
                    for ik, iv in v.items():
                        if isinstance(iv, str): inner[ik] = {"stringValue": iv}
                    map_fields[k] = {"mapValue": {"fields": inner}}
            fields[key] = {"mapValue": {"fields": map_fields}}
        elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
        elif isinstance(val, bool): fields[key] = {"booleanValue": val}
    body = {"fields": fields}
    r = requests.patch(url, json=body)
    if r.status_code in [200, 201]: return True
    create_url = f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}"
    r2 = requests.post(create_url, json=body)
    return r2.status_code in [200, 201]

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

def firestore_delete(collection, doc_id):
    return requests.delete(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}").status_code == 200

def send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode or "Markdown"}
    if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
    return requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload)

def send_document(chat_id, file_content, filename):
    files = {'document': (filename, file_content)}
    return requests.post(f"{TELEGRAM_URL}/sendDocument", data={'chat_id': chat_id}, files=files)

# === НАПОМИНАНИЯ ===
def reminder_worker():
    while True:
        try:
            now = datetime.now()
            for hours in [24, 3, 1]:
                rt = (now + timedelta(hours=hours)).strftime('%H:%M')
                cd = (now + timedelta(hours=hours)).strftime('%Y-%m-%d')
                for a in firestore_query("appointments", "date", "EQUAL", cd):
                    if a.get("status") != "confirmed": continue
                    if a.get("time") == rt and not a.get(f"reminded_{hours}h"):
                        if hours == 1:
                            send_message(int(a["master_id"]), f"⏰ Через час: {a.get('client_name')} — {a.get('service')}")
                        if "client_id" in a:
                            send_message(int(a["client_id"]), f"⏰ Напоминание! {a.get('service')} в {a.get('time')}")
                        firestore_set("appointments", a["_id"], {f"reminded_{hours}h": True})
        except Exception as e: print(f"Reminder: {e}")
        time.sleep(60)

threading.Thread(target=reminder_worker, daemon=True).start()

STATES = {}

def master_menu():
    return {"keyboard": [["📊 Дашборд", "📅 Расписание"], ["➕ Новая запись", "👥 Клиенты"], ["🔗 Моя ссылка", "📢 Свободные окна"], ["⚙️ Настройки", "❓ Помощь"]], "resize_keyboard": True}

def client_menu():
    return {"keyboard": [["📋 Мои записи"], ["🔍 Найти мастера"], ["❓ Помощь"]], "resize_keyboard": True}

def settings_menu():
    return {"keyboard": [["💈 Услуги", "⏰ Часы"], ["🚫 Перерывы", "📍 Адрес"], ["🖼 Портфолио", "🚷 Чёрный список"], ["🔙 В меню"]], "resize_keyboard": True}

# === ПОМОЩЬ ===
def handle_help(chat_id, is_master):
    if is_master:
        send_message(chat_id, "📖 *Помощь*\n📊 Дашборд — доход\n📅 Расписание — записи\n👥 Клиенты — список\n🔗 Моя ссылка — отправить клиентам\n⚙️ Настройки — услуги, часы, адрес")
    else:
        send_message(chat_id, "📖 *Помощь*\n📋 Мои записи\n🔍 Найти мастера по номеру")

# === СТАРТ ===
def handle_start(chat_id, user_name):
    if firestore_get("masters", str(chat_id)):
        send_message(chat_id, f"👋 {user_name}!", reply_markup=master_menu())
    elif firestore_get("clients", str(chat_id)):
        send_message(chat_id, f"👋 {user_name}!", reply_markup=client_menu())
    else:
        send_message(chat_id, "👋 *График.Про*\n\nКто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})

def handle_master_registration(chat_id, user_name, username):
    firestore_set("masters", str(chat_id), {"name": user_name, "username": username, "phone": "", "services": [], "schedule": {"monday":{"start":"09:00","end":"18:00"},"tuesday":{"start":"09:00","end":"18:00"},"wednesday":{"start":"09:00","end":"18:00"},"thursday":{"start":"09:00","end":"18:00"},"friday":{"start":"09:00","end":"18:00"},"saturday":{"start":"10:00","end":"15:00"},"sunday":{"start":"10:00","end":"15:00"}}, "address": "", "completed_onboarding": False, "blacklist": [], "portfolio": [], "rating": 0, "ratings_count": 0})
    send_message(chat_id, f"✅ {user_name}, добро пожаловать!")
    start_onboarding(chat_id)

def start_onboarding(chat_id):
    send_message(chat_id, "👋 *Настроим кабинет!*\n\n*Шаг 1:* Добавьте услуги", reply_markup={"inline_keyboard": [[{"text": "💈 Добавить услуги", "callback_data": "addservice"}], [{"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}]]})

def finish_onboarding(chat_id):
    firestore_set("masters", str(chat_id), {"completed_onboarding": True})
    send_message(chat_id, "🎉 *Готово!*", reply_markup=master_menu())
    handle_master_link(chat_id)

# === УСЛУГИ ===
def handle_services_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    buttons = [[{"text": f"❌ {s['name']} ({s.get('price',0)}₽, {s.get('duration',60)}мин)", "callback_data": f"delservice_{s['name']}"}] for s in services]
    buttons.append([{"text": "➕ Добавить", "callback_data": "addservice"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    send_message(chat_id, "💈 *Услуги:*" if services else "💈 Нет услуг.", reply_markup={"inline_keyboard": buttons})

def handle_add_service_start(chat_id):
    STATES[str(chat_id)] = {"state": "adding_service_name"}
    send_message(chat_id, "✏️ Название услуги:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_add_service_name(chat_id, name):
    STATES[str(chat_id)] = {"state": "adding_service_price", "name": name}
    send_message(chat_id, f"💰 Цена для «{name}»:")

def handle_add_service_price(chat_id, price_text):
    state = STATES.get(str(chat_id), {})
    try: price = int(price_text.strip())
    except: return send_message(chat_id, "❌ Введите число.")
    STATES[str(chat_id)] = {"state": "adding_service_duration", "name": state.get("name"), "price": price}
    send_message(chat_id, f"⏱ Длительность «{state.get('name')}» в минутах:")

def handle_add_service_duration(chat_id, dur_text):
    state = STATES.get(str(chat_id), {})
    if not state: return send_message(chat_id, "❌ Сессия истекла.", reply_markup=settings_menu())
    try: duration = int(dur_text.strip())
    except: return send_message(chat_id, "❌ Введите число.")
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "❌ Зарегистрируйтесь: /start")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    services.append({"name": state["name"], "price": state["price"], "duration": duration})
    firestore_set("masters", str(chat_id), {"services": services})
    STATES.pop(str(chat_id), None)
    if not master.get("completed_onboarding"):
        firestore_set("masters", str(chat_id), {"completed_onboarding": True})
        send_message(chat_id, "✅ *Отлично!*\nТеперь: ⚙️ Настройки → ⏰ Часы", reply_markup=master_menu())
    else:
        send_message(chat_id, f"✅ *{state['name']}* — {state['price']}₽, {duration}мин", reply_markup=settings_menu())

def handle_delete_service(chat_id, name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") != name]
        firestore_set("masters", str(chat_id), {"services": services})
    handle_services_settings(chat_id)

# === ЧАСЫ ===
def handle_schedule_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    schedule = master.get("schedule", {})
    days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    day_names = ["ПН","ВТ","СР","ЧТ","ПТ","СБ","ВС"]
    text = "⏰ *Часы:*\n"
    for i, d in enumerate(days):
        s = schedule.get(d)
        if isinstance(s, dict) and s.get("start"):
            text += f"{day_names[i]}: {s['start']} – {s['end']}\n"
        else:
            text += f"{day_names[i]}: выходной\n"
    text += "\n`ПН 09:00-18:00` или `ВС выходной`"
    STATES[str(chat_id)] = {"state": "setting_day_schedule"}
    send_message(chat_id, text, reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_day_schedule_set(chat_id, text):
    days_map = {"ПН":"monday","ВТ":"tuesday","СР":"wednesday","ЧТ":"thursday","ПТ":"friday","СБ":"saturday","ВС":"sunday"}
    parts = text.strip().split()
    if len(parts) < 2: return send_message(chat_id, "❌ Формат: ПН 09:00-18:00")
    day_code = parts[0].upper()
    day_key = days_map.get(day_code)
    if not day_key: return send_message(chat_id, "❌ Неверный день.")
    time_part = " ".join(parts[1:])
    master = firestore_get("masters", str(chat_id))
    schedule = master.get("schedule", {})
    if time_part.lower() == "выходной":
        schedule[day_key] = {}
    else:
        if "-" not in time_part: return send_message(chat_id, "❌ Формат: 09:00-18:00")
        start, end = time_part.split("-")
        schedule[day_key] = {"start": start.strip(), "end": end.strip()}
    firestore_set("masters", str(chat_id), {"schedule": schedule})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ {day_code} обновлён!", reply_markup=settings_menu())

# === АДРЕС ===
def handle_address_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    current = master.get("address", "Не указан")
    STATES[str(chat_id)] = {"state": "setting_address"}
    send_message(chat_id, f"📍 *Адрес*\nСейчас: {current}\n\nОтправьте новый адрес:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_address_set(chat_id, text):
    firestore_set("masters", str(chat_id), {"address": text})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Адрес сохранён: {text}", reply_markup=settings_menu())

# === ССЫЛКА ===
def handle_master_link(chat_id):
    if not firestore_get("masters", str(chat_id)): return send_message(chat_id, "Сначала зарегистрируйтесь.")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": str(chat_id)})
    send_message(chat_id, f"🔗 *Твоя ссылка:*\n\n[Открыть в Telegram](tg://resolve?domain=grafikpro_bot&start=master_{link_id})\n\n`https://t.me/grafikpro_bot?start=master_{link_id}`")

# === КЛИЕНТ: ЗАПИСЬ ===
def handle_client_start(chat_id, link_id):
    if not firestore_get("clients", str(chat_id)): firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
    link = firestore_get("links", link_id)
    if not link: return send_message(chat_id, "❌ Ссылка недействительна.")
    master = firestore_get("masters", link["master_id"])
    if not master: return send_message(chat_id, "❌ Мастер не найден.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not services: return send_message(chat_id, "❌ Нет услуг.")
    addr = master.get("address", "")
    text = f"📝 *Запись к {master.get('name')}*"
    if addr: text += f"\n📍 {addr}"
    text += "\nВыберите услугу:"
    buttons = [[{"text": f"{s['name']} — {s['price']}₽ ({s.get('duration',60)}мин)", "callback_data": f"clsrv_{link_id}_{s['name']}"}] for s in services]
    STATES[str(chat_id)] = {"link_id": link_id, "master_name": master.get("name"), "master_addr": addr}
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_client_service_select(chat_id, link_id, service_name):
    STATES[str(chat_id)] = STATES.get(str(chat_id), {})
    STATES[str(chat_id)]["service"] = service_name
    STATES[str(chat_id)]["state"] = "client_picking_date"
    buttons = [[{"text": (datetime.now() + timedelta(days=i+1)).strftime('%d.%m (%a)'), "callback_data": f"cldate_{link_id}_{(datetime.now() + timedelta(days=i+1)).strftime('%Y-%m-%d')}"}] for i in range(7)]
    send_message(chat_id, f"📅 *{service_name}*\nВыберите дату:", reply_markup={"inline_keyboard": buttons})

def handle_client_date_select(chat_id, link_id, date):
    link = firestore_get("links", link_id)
    if not link: return send_message(chat_id, "❌ Ссылка не найдена.")
    master = firestore_get("masters", link["master_id"])
    schedule = master.get("schedule", {})
    try:
        day_key = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][datetime.strptime(date, '%Y-%m-%d').weekday()]
        day_sched = schedule.get(day_key)
    except:
        day_sched = None
    if not day_sched or not isinstance(day_sched, dict) or not day_sched.get("start"):
        return send_message(chat_id, "📭 Выходной.")
    start_h = int(day_sched["start"].split(":")[0])
    end_h = int(day_sched["end"].split(":")[0])
    busy = {a.get("time") for a in firestore_query("appointments", "master_id", "EQUAL", link["master_id"]) if a.get("date") == date and a.get("status") != "cancelled"}
    buttons = []
    for h in range(start_h, end_h + 1):
        slot = f"{h}:00"
        if slot in busy: buttons.append([{"text": f"❌ {slot}", "callback_data": "ignore"}])
        else: buttons.append([{"text": f"🟢 {slot}", "callback_data": f"cltime_{link_id}_{date}_{slot}"}])
    if not buttons: return send_message(chat_id, "📭 Нет слотов.")
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
    state = STATES.pop(str(chat_id), {})
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    if len(phone_clean) < 10: return send_message(chat_id, "❌ Неверный формат.")
    link = firestore_get("links", state.get("link_id", ""))
    if not link: return send_message(chat_id, "❌ Сессия истекла.")
    master = firestore_get("masters", link["master_id"])
    addr = master.get("address", "")
    firestore_add("appointments", {"master_id": link["master_id"], "client_id": str(chat_id), "client_name": state["client_name"], "client_phone": phone_clean, "service": state["service"], "date": state["date"], "time": state["time"], "status": "confirmed"})
    ics = f"BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nDTSTART:{state['date'].replace('-','')}T{state['time'].replace(':','')}00\nSUMMARY:{master.get('name')} - {state.get('service')}\nEND:VEVENT\nEND:VCALENDAR"
    send_document(chat_id, ics.encode(), "appointment.ics")
    text = f"✅ *Запись подтверждена!*\n\n{master.get('name')}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}"
    if addr: text += f"\n📍 {addr}"
    send_message(chat_id, text + f"\n\n{state.get('client_name')} | {phone_clean}", reply_markup=client_menu())
    send_message(int(link["master_id"]), f"🔔 *Новая запись!*\n\n{state.get('client_name')}\n{phone_clean}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}")

# === КЛИЕНТСКИЙ КАБИНЕТ ===
def handle_client_appointments(chat_id):
    all_appts = firestore_query("appointments", "client_id", "EQUAL", str(chat_id))
    active = [a for a in all_appts if a.get("status") not in ["cancelled"]]
    if not active: return send_message(chat_id, "📋 У вас пока нет записей.")
    active.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    text = "📋 *Мои записи:*\n"
    buttons = []
    for a in active:
        master = firestore_get("masters", a.get("master_id", ""))
        master_name = master.get("name", "Мастер") if master else "Мастер"
        addr = master.get("address", "") if master else ""
        text += f"\n• {a.get('date')} в {a.get('time')}\n  {a.get('service')} у {master_name}"
        if addr: text += f"\n  📍 {addr}"
        if a.get("status") == "confirmed":
            buttons.append([{"text": f"❌ Отменить: {a.get('date')} {a.get('time')}", "callback_data": f"cancel_{a['_id']}"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_cancel_appointment(chat_id, appt_id):
    appt = firestore_get("appointments", appt_id)
    if not appt or appt.get("client_id") != str(chat_id): return send_message(chat_id, "❌ Ошибка.")
    if appt.get("master_id"): send_message(int(appt["master_id"]), f"❌ *Отмена!*\n{appt.get('client_name')} отменил {appt.get('service')} {appt.get('date')} в {appt.get('time')}")
    firestore_set("appointments", appt_id, {"status": "cancelled"})
    send_message(chat_id, "✅ Отменено.", reply_markup=client_menu())

# === ПОИСК МАСТЕРА ===
def handle_find_master_start(chat_id):
    STATES[str(chat_id)] = {"state": "finding_master"}
    send_message(chat_id, "🔍 Введите номер телефона мастера:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_find_master(chat_id, phone):
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    masters = firestore_query("masters", "phone", "EQUAL", phone_clean)
    if not masters: STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Мастер не найден.", reply_markup=client_menu())
    master = masters[0]
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    addr = master.get("address", "Не указан")
    text = f"👤 *{master.get('name')}*\n📍 {addr}\n\n💈 *Услуги:*\n" + "\n".join([f"• {s['name']} — {s['price']}₽" for s in services]) if services else ""
    links = firestore_query("links", "master_id", "EQUAL", master.get("_id", ""))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": master.get("_id", "")})
    STATES.pop(str(chat_id), None)
    buttons = [[{"text": "📝 Записаться", "callback_data": f"clsrv_{link_id}_{services[0]['name']}"}]] if services else []
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

# === СВОБОДНЫЕ ОКНА ===
def handle_free_slots(chat_id):
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    master = firestore_get("masters", str(chat_id))
    schedule = master.get("schedule", {})
    day_key = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][(datetime.now() + timedelta(days=1)).weekday()]
    day_sched = schedule.get(day_key)
    if not day_sched or not day_sched.get("start"): return send_message(chat_id, "📭 Выходной.")
    start_h = int(day_sched["start"].split(":")[0])
    end_h = int(day_sched["end"].split(":")[0])
    busy = {a.get("time") for a in firestore_query("appointments", "master_id", "EQUAL", str(chat_id)) if a.get("date") == tomorrow}
    free = [f"{h}:00" for h in range(start_h, end_h + 1) if f"{h}:00" not in busy]
    if not free: return send_message(chat_id, "📭 Всё занято!")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    send_message(chat_id, f"🟢 *Окна на завтра:*\n{', '.join(free)}\n\nhttps://t.me/grafikpro_bot?start=master_{link_id}")

# === ДАШБОРД ===
def handle_dashboard(chat_id):
    today = datetime.now().strftime('%Y-%m-%d')
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    today_appts = [a for a in appointments if a.get("date") == today and a.get("status") != "cancelled"]
    master = firestore_get("masters", str(chat_id)) or {}
    services = master.get("services", [])
    total = 0
    for a in today_appts:
        svc = a.get("service", "")
        for s in services:
            if isinstance(s, dict) and s.get("name") == svc: total += s.get("price", 0)
    send_message(chat_id, f"📊 *Дашборд на сегодня*\n\n📅 Записей: {len(today_appts)}\n💰 Доход: {total}₽")

# === РАСПИСАНИЕ ===
def handle_schedule_view(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "📭 Записей пока нет.")
    appointments.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    today = datetime.now().strftime('%Y-%m-%d')
    text = "📅 *Расписание:*\n"
    buttons = []
    for a in appointments:
        if a.get("status") == "cancelled": continue
        date_str = "Сегодня" if a.get("date") == today else a.get("date", "?")
        status_icon = {"confirmed": "🟡", "completed": "✅", "no_show": "❌"}.get(a.get("status"), "")
        text += f"\n{status_icon} {date_str} {a.get('time')} — {a.get('service')}\n  {a.get('client_name','?')} | {a.get('client_phone','?')}"
        if a.get("status") == "confirmed":
            buttons.append([{"text": f"✅ Вып: {a.get('date')} {a.get('time')}", "callback_data": f"complete_{a['_id']}"}])
            buttons.append([{"text": f"❌ Неявка: {a.get('date')} {a.get('time')}", "callback_data": f"noshow_{a['_id']}"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_complete_appointment(chat_id, appt_id):
    firestore_set("appointments", appt_id, {"status": "completed"})
    send_message(chat_id, "✅ Выполнено!", reply_markup=master_menu())

def handle_noshow_appointment(chat_id, appt_id):
    firestore_set("appointments", appt_id, {"status": "no_show"})
    send_message(chat_id, "❌ Неявка.", reply_markup=master_menu())

# === КЛИЕНТЫ ===
def handle_clients_list(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "👥 Пока нет клиентов.")
    clients = {}
    for a in appointments:
        cid = a.get("client_phone", "")
        if cid not in clients: clients[cid] = {"name": a.get("client_name", "?"), "phone": cid, "history": []}
        clients[cid]["history"].append(f"{a.get('service')} ({a.get('date')})")
    text = "👥 *Клиенты:*\n"
    for cid, data in list(clients.items())[:10]:
        text += f"\n• *{data['name']}* | {data['phone']}\n  {', '.join(data['history'][-3:])}"
    send_message(chat_id, text)

# === ОБРАБОТКА ===
def handle_text(chat_id, user_name, username, text):
    state = STATES.get(str(chat_id), {}).get("state")
    if state == "adding_service_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_name(chat_id, text)
    if state == "adding_service_price":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_price(chat_id, text)
    if state == "adding_service_duration":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_duration(chat_id, text)
    if state == "client_entering_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.")
        return handle_client_name(chat_id, text)
    if state == "client_entering_phone":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.")
        return handle_client_phone(chat_id, text)
    if state == "setting_day_schedule":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_day_schedule_set(chat_id, text)
    if state == "setting_address":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_address_set(chat_id, text)
    if state == "finding_master":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=client_menu())
        return handle_find_master(chat_id, text)
    
    is_master = firestore_get("masters", str(chat_id))
    if text == "👤 Я мастер": handle_master_registration(chat_id, user_name, username)
    elif text == "👥 Я клиент":
        if not firestore_get("clients", str(chat_id)): firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
        send_message(chat_id, "👥 *Клиентский кабинет*", reply_markup=client_menu())
    elif text == "📊 Дашборд": handle_dashboard(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "📋 Мои записи": handle_client_appointments(chat_id)
    elif text == "🔍 Найти мастера": handle_find_master_start(chat_id)
    elif text == "📢 Свободные окна": handle_free_slots(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "⚙️ Настройки": send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Услуги": handle_services_settings(chat_id)
    elif text == "⏰ Часы": handle_schedule_settings(chat_id)
    elif text == "📍 Адрес": handle_address_settings(chat_id)
    elif text == "🔙 В меню": STATES.pop(str(chat_id), None); send_message(chat_id, "Главное меню", reply_markup=master_menu() if is_master else client_menu())
    elif text == "🔗 Моя ссылка": handle_master_link(chat_id)
    elif text == "📅 Расписание": handle_schedule_view(chat_id)
    elif text == "👥 Клиенты": handle_clients_list(chat_id)
    elif text == "❓ Помощь": handle_help(chat_id, is_master)
    else: send_message(chat_id, "Используйте меню.", reply_markup=master_menu() if is_master else client_menu())

def handle_callback(chat_id, data):
    if data == "addservice": handle_add_service_start(chat_id)
    elif data.startswith("delservice_"): handle_delete_service(chat_id, data.replace("delservice_", "", 1))
    elif data == "settings_back": send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data.startswith("cancel_"): handle_cancel_appointment(chat_id, data.replace("cancel_", "", 1))
    elif data.startswith("complete_"): handle_complete_appointment(chat_id, data.replace("complete_", "", 1))
    elif data.startswith("noshow_"): handle_noshow_appointment(chat_id, data.replace("noshow_", "", 1))
    elif data.startswith("clsrv_"): handle_client_service_select(chat_id, data.split("_")[1], data.split("_")[2])
    elif data.startswith("cldate_"): handle_client_date_select(chat_id, data.split("_")[1], data.split("_")[2])
    elif data.startswith("cltime_"): handle_client_time_select(chat_id, data.split("_")[1], data.split("_")[2], data.split("_")[3])
    elif data == "onboarding_skip": finish_onboarding(chat_id)
    elif data == "ignore": pass

def process_update(update):
    if "message" in update:
        msg = update["message"]
        chat_id, text = msg["chat"]["id"], msg.get("text", "")
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

app = handler