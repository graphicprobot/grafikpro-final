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

# === БАЗА ДАННЫХ ===
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
    
    # Пробуем сначала PATCH (обновить существующий)
    update_fields = list(data.keys())
    patch_url = f"{url}&updateMask.fieldPaths={'&updateMask.fieldPaths='.join(update_fields)}"
    
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
            fields[key] = {"mapValue": {"fields": map_fields}}
        elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
    
    body = {"fields": fields}
    
    # Пробуем PATCH
    r = requests.patch(patch_url, json=body)
    if r.status_code in [200, 201]:
        return True
    
    # Если не найден — создаём через POST с указанием documentId
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

# === TELEGRAM API ===
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
            reminder_time = (now + timedelta(hours=1)).strftime('%H:%M')
            tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')
            for date in [now.strftime('%Y-%m-%d'), tomorrow]:
                all_appts = firestore_query("appointments", "date", "EQUAL", date)
                for a in all_appts:
                    if a.get("time") == reminder_time and a.get("status") == "confirmed" and not a.get("reminded"):
                        send_message(int(a["master_id"]), f"⏰ *Напоминание!*\nЧерез час запись:\n{a.get('client_name')} — {a.get('service')}\n{a.get('date')} в {a.get('time')}")
                        if "client_id" in a:
                            send_message(int(a["client_id"]), f"⏰ *Напоминание!*\nСегодня в {a.get('time')} у вас запись:\n{a.get('service')}")
                        firestore_set("appointments", a["_id"], {"reminded": True})
        except Exception as e:
            print(f"Reminder error: {e}")
        time.sleep(60)

threading.Thread(target=reminder_worker, daemon=True).start()

# === КЛАВИАТУРЫ ===
def master_menu():
    return {"keyboard": [["📊 Дашборд", "📅 Расписание"], ["➕ Новая запись", "👥 Клиенты"], ["🔗 Моя ссылка", "⚙️ Настройки"], ["📢 Свободные окна"]], "resize_keyboard": True}

def client_menu():
    return {"keyboard": [["📋 Мои записи"], ["🔍 Найти мастера"]], "resize_keyboard": True}

def settings_menu():
    return {"keyboard": [["💈 Услуги", "⏰ Часы"], ["🚫 Перерывы", "🔙 В меню"]], "resize_keyboard": True}

STATES = {}

# === СТАРТ + ОНБОРДИНГ ===
def handle_start(chat_id, user_name):
    if firestore_get("masters", str(chat_id)):
        send_message(chat_id, f"👋 {user_name}!", reply_markup=master_menu())
    elif firestore_get("clients", str(chat_id)):
        send_message(chat_id, f"👋 {user_name}!", reply_markup=client_menu())
    else:
        send_message(chat_id, "👋 *График.Про*\n\nКто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})

def handle_master_registration(chat_id, user_name, username):
    firestore_set("masters", str(chat_id), {
        "name": user_name,
        "username": username,
        "services": [],
        "schedule": {"start": "09:00", "end": "18:00"},
        "completed_onboarding": False,
        "created_at": datetime.now().isoformat()
    })
    send_message(chat_id, f"✅ {user_name}, добро пожаловать!")
    start_onboarding(chat_id)

def start_onboarding(chat_id):
    text = "👋 *Давай настроим твой кабинет!*\n\nЭто займёт 2 минуты.\n\n*Шаг 1:* Добавь услуги (цены и длительность)"
    buttons = [[{"text": "💈 Добавить услуги", "callback_data": "addservice"}]]
    STATES[str(chat_id)] = {"onboarding_step": 1}
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

# === ФИНАНСОВАЯ АНАЛИТИКА ===
def handle_dashboard(chat_id, period="today"):
    today = datetime.now().strftime('%Y-%m-%d')
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    master = firestore_get("masters", str(chat_id)) or {}
    services = master.get("services", [])
    
    if period == "today":
        filtered = [a for a in appointments if a.get("date") == today]
        label = "📊 *Дашборд на сегодня*"
    elif period == "week":
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        prev_ago = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        filtered = [a for a in appointments if a.get("date", "") >= week_ago]
        prev_filtered = [a for a in appointments if prev_ago <= a.get("date", "") < week_ago]
        label = "📊 *Доход за неделю*"
    else:
        month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        prev_ago = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        filtered = [a for a in appointments if a.get("date", "") >= month_ago]
        prev_filtered = [a for a in appointments if prev_ago <= a.get("date", "") < month_ago]
        label = "📊 *Доход за месяц*"
    
    def calc_total(appts):
        total = 0
        for a in appts:
            svc = a.get("service", "")
            for s in services:
                if isinstance(s, dict) and s.get("name") == svc:
                    total += s.get("price", 0)
        return total
    
    total = calc_total(filtered)
    prev_total = calc_total(prev_filtered) if period != "today" else 0
    
    text = f"{label}\n\n📅 Записей: *{len(filtered)}*\n💰 Доход: *{total}₽*"
    if period != "today" and prev_total > 0:
        change = int((total - prev_total) / prev_total * 100)
        emoji = "📈" if change > 0 else "📉"
        text += f"\n\n{emoji} *{change:+d}%* к прошлому периоду"
    
    buttons = [[{"text": "Сегодня", "callback_data": "dash_today"}, {"text": "Неделя", "callback_data": "dash_week"}, {"text": "Месяц", "callback_data": "dash_month"}]]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

# === СВОБОДНЫЕ ОКНА ===
def handle_free_slots(chat_id):
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    master = firestore_get("masters", str(chat_id))
    sched = master.get("schedule", {"start": "09:00", "end": "18:00"})
    start_h, end_h = int(sched["start"].split(":")[0]), int(sched["end"].split(":")[0])
    busy = {a.get("time") for a in firestore_query("appointments", "master_id", "EQUAL", str(chat_id)) if a.get("date") == tomorrow}
    breaks = master.get("breaks", [])
    free = []
    for h in range(start_h, end_h + 1):
        slot = f"{h}:00"
        if slot not in busy and not any(slot >= b.split("-")[0] and slot < b.split("-")[1] for b in breaks):
            free.append(slot)
    
    if not free: return send_message(chat_id, "📭 На завтра всё занято!")
    
    bot_user = "grafikpro_bot"
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    
    text = f"🟢 *Свободные окна на завтра ({tomorrow}):*\n\n"
    for slot in free:
        text += f"• {slot}\n"
    text += f"\n*Пост для соцсетей:*\n```\nСвободные окна на завтра:\n{', '.join(free)}\n\nЗапись: https://t.me/{bot_user}?start=master_{link_id}\n```"
    send_message(chat_id, text)

# === ШАБЛОНЫ ===
def handle_service_template(chat_id, service_name):
    master = firestore_get("masters", str(chat_id))
    services = master.get("services", [])
    svc = next((s for s in services if isinstance(s, dict) and s.get("name") == service_name), None)
    if not svc: return
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    text = f"💈 *{svc['name']}* — {svc['price']}₽, {svc['duration']}мин\n\nЗапись: https://t.me/grafikpro_bot?start=master_{link_id}"
    send_message(chat_id, text)

# === РУЧНАЯ ЗАПИСЬ + ЗАМЕТКИ ===
def handle_manual_appointment_start(chat_id):
    STATES[str(chat_id)] = {"state": "manual_name"}
    send_message(chat_id, "📝 *Новая запись*\nВведите имя клиента:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_manual_name(chat_id, name):
    STATES[str(chat_id)] = {"state": "manual_phone", "client_name": name}
    send_message(chat_id, f"📞 Телефон клиента:")

def handle_manual_phone(chat_id, phone):
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    if len(phone_clean) < 10: return send_message(chat_id, "❌ Неверный формат.")
    STATE = STATES[str(chat_id)]
    STATE["state"] = "manual_service"
    STATE["client_phone"] = phone_clean
    
    # Показываем историю и заметки
    history = firestore_query("appointments", "client_phone", "EQUAL", phone_clean)
    notes = firestore_get("masters", str(chat_id)).get("client_notes", {})
    client_note = notes.get(phone_clean, "")
    
    if history or client_note:
        hist_text = "📋 *История клиента:*\n"
        for h in history[-5:]:
            hist_text += f"• {h.get('date')} — {h.get('service')}\n"
        if client_note:
            hist_text += f"\n📝 *Заметка:* {client_note}"
        send_message(chat_id, hist_text)
    
    master = firestore_get("masters", str(chat_id))
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not services: return send_message(chat_id, "❌ Сначала добавьте услуги.")
    buttons = [[{"text": f"{s['name']} ({s['price']}₽)", "callback_data": f"man_srv_{s['name']}"}] for s in services]
    buttons.append([{"text": "📝 Добавить заметку", "callback_data": f"add_note_{phone_clean}"}])
    send_message(chat_id, "💈 Выберите услугу:", reply_markup={"inline_keyboard": buttons})

def handle_add_client_note(chat_id, phone):
    STATES[str(chat_id)] = {"state": "adding_note", "note_phone": phone}
    send_message(chat_id, "📝 Введите заметку о клиенте:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_save_note(chat_id, text):
    phone = STATES.get(str(chat_id), {}).get("note_phone", "")
    master = firestore_get("masters", str(chat_id)) or {}
    notes = master.get("client_notes", {})
    notes[phone] = text
    firestore_set("masters", str(chat_id), {"client_notes": notes})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Заметка сохранена!", reply_markup=master_menu())

def handle_manual_service(chat_id, service_name):
    STATES[str(chat_id)]["service"] = service_name
    STATES[str(chat_id)]["state"] = "manual_date"
    buttons = [[{"text": ("Сегодня" if i==0 else (datetime.now()+timedelta(days=i)).strftime('%d.%m (%a)')), "callback_data": f"man_date_{(datetime.now()+timedelta(days=i)).strftime('%Y-%m-%d')}"}] for i in range(14)]
    send_message(chat_id, "📅 Выберите дату:", reply_markup={"inline_keyboard": buttons})

def handle_manual_date(chat_id, date):
    STATES[str(chat_id)]["date"] = date
    STATES[str(chat_id)]["state"] = "manual_time"
    master = firestore_get("masters", str(chat_id))
    sched = master.get("schedule", {"start": "09:00", "end": "18:00"})
    start_h, end_h = int(sched["start"].split(":")[0]), int(sched["end"].split(":")[0])
    busy = {a.get("time") for a in firestore_query("appointments", "master_id", "EQUAL", str(chat_id)) if a.get("date") == date}
    buttons = [[{"text": f"🟢 {h}:00", "callback_data": f"man_time_{h}:00"}] if f"{h}:00" not in busy else [{"text": f"❌ {h}:00", "callback_data": "ignore"}] for h in range(start_h, end_h+1)]
    send_message(chat_id, f"⏰ Время на {date}:", reply_markup={"inline_keyboard": buttons})

def handle_manual_time(chat_id, time):
    state = STATES.pop(str(chat_id), {})
    firestore_add("appointments", {"master_id": str(chat_id), "client_name": state.get("client_name",""), "client_phone": state.get("client_phone",""), "service": state.get("service",""), "date": state.get("date",""), "time": time, "status": "confirmed"})
    send_message(chat_id, f"✅ Запись добавлена!\n\n{state.get('client_name')}\n{state.get('service')}\n{state.get('date')} в {time}", reply_markup=master_menu())

# === КЛИЕНТСКАЯ ЧАСТЬ ===
def handle_client_start(chat_id, link_id):
    if not firestore_get("clients", str(chat_id)): firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
    link = firestore_get("links", link_id)
    if not link: return send_message(chat_id, "❌ Ссылка недействительна.")
    master = firestore_get("masters", link["master_id"])
    if not master: return send_message(chat_id, "❌ Мастер не найден.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not services: return send_message(chat_id, "❌ Нет услуг.")
    buttons = [[{"text": f"{s['name']} — {s['price']}₽ ({s.get('duration',60)}мин)", "callback_data": f"clsrv_{link_id}_{s['name']}"}] for s in services]
    STATES[str(chat_id)] = {"link_id": link_id, "master_name": master.get("name")}
    send_message(chat_id, f"📝 *Запись к {master.get('name')}*\nВыберите услугу:", reply_markup={"inline_keyboard": buttons})

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
    sched = master.get("schedule", {"start": "09:00", "end": "18:00"})
    start_h, end_h = int(sched["start"].split(":")[0]), int(sched["end"].split(":")[0])
    breaks = master.get("breaks", [])
    busy = {a.get("time") for a in firestore_query("appointments", "master_id", "EQUAL", link["master_id"]) if a.get("date") == date}
    buttons = []
    for h in range(start_h, end_h+1):
        slot = f"{h}:00"
        if slot in busy: buttons.append([{"text": f"❌ {slot}", "callback_data": "ignore"}])
        elif any(slot >= b.split("-")[0] and slot < b.split("-")[1] for b in breaks): buttons.append([{"text": f"🚫 {slot}", "callback_data": "ignore"}])
        else: buttons.append([{"text": f"🟢 {slot}", "callback_data": f"cltime_{link_id}_{date}_{slot}"}])
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
    firestore_add("appointments", {"master_id": link["master_id"], "client_id": str(chat_id), "client_name": state["client_name"], "client_phone": phone_clean, "service": state["service"], "date": state["date"], "time": state["time"], "status": "confirmed"})
    
    # Генерация .ics файла
    ics_content = f"BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nDTSTART:{state['date'].replace('-','')}T{state['time'].replace(':','')}00\nDTEND:{state['date'].replace('-','')}T{state['time'].replace(':','')}00\nSUMMARY:{master.get('name')} - {state.get('service')}\nEND:VEVENT\nEND:VCALENDAR"
    send_document(chat_id, ics_content.encode(), "appointment.ics")
    
    send_message(chat_id, f"✅ *Запись подтверждена!*\n\n{master.get('name')}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}\n{state.get('client_name')} | {phone_clean}\n\n📅 Файл для календаря прикреплён выше.", reply_markup=client_menu())
    send_message(int(link["master_id"]), f"🔔 *Новая запись!*\n\n{state.get('client_name')}\n{phone_clean}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}")

# === КЛИЕНТСКИЙ КАБИНЕТ ===
def handle_client_appointments(chat_id):
    all_appts = firestore_query("appointments", "client_id", "EQUAL", str(chat_id))
    if not all_appts: return send_message(chat_id, "📋 У вас пока нет записей.")
    all_appts.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    text = "📋 *Мои записи:*\n"
    buttons = []
    for a in all_appts:
        master = firestore_get("masters", a.get("master_id", ""))
        master_name = master.get("name", "Мастер") if master else "Мастер"
        text += f"\n• {a.get('date')} в {a.get('time')}\n  {a.get('service')} у {master_name}"
        buttons.append([{"text": f"❌ Отменить: {a.get('date')} {a.get('time')}", "callback_data": f"cancel_{a['_id']}"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_cancel_appointment(chat_id, appt_id):
    appt = firestore_get("appointments", appt_id)
    if not appt or appt.get("client_id") != str(chat_id): return send_message(chat_id, "❌ Ошибка.")
    if appt.get("master_id"): send_message(int(appt["master_id"]), f"❌ *Отмена записи!*\n\n{appt.get('client_name')} отменил запись\n{appt.get('service')}\n{appt.get('date')} в {appt.get('time')}")
    firestore_delete("appointments", appt_id)
    send_message(chat_id, "✅ Запись отменена.", reply_markup=client_menu())

# === ОСТАЛЬНОЕ ===
def handle_master_link(chat_id):
    if not firestore_get("masters", str(chat_id)): return send_message(chat_id, "Сначала зарегистрируйтесь.")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": str(chat_id)})
    bot_user = "grafikpro_bot"
    send_message(chat_id, f"🔗 *Ссылка для клиентов:*\n\n📨 [Открыть в Telegram](tg://resolve?domain={bot_user}&start=master_{link_id})\n\nИли скопируйте:\n`https://t.me/{bot_user}?start=master_{link_id}`")

def handle_schedule_view(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "📭 Записей пока нет.")
    appointments.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    today = datetime.now().strftime('%Y-%m-%d')
    text = "📅 *Расписание:*\n"
    for a in appointments:
        date_str = "Сегодня" if a.get("date") == today else a.get("date", "?")
        text += f"\n• {date_str} {a.get('time')} — {a.get('service')}\n  {a.get('client_name','?')} | {a.get('client_phone','?')}"
    send_message(chat_id, text)

def handle_clients_list(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "👥 Пока нет клиентов.")
    clients = {}
    for a in appointments:
        cid = a.get("client_phone", a.get("client_id"))
        if cid not in clients:
            clients[cid] = {"name": a.get("client_name", "?"), "phone": a.get("client_phone", ""), "history": []}
        clients[cid]["history"].append(f"{a.get('service')} ({a.get('date')})")
    text = "👥 *Клиенты:*\n"
    for cid, data in list(clients.items())[:10]:
        text += f"\n• *{data['name']}* | {data['phone']}\n  {', '.join(data['history'][-3:])}"
    send_message(chat_id, text)

def handle_services_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    buttons = []
    for s in services:
        buttons.append([{"text": f"❌ {s['name']} ({s.get('price',0)}₽)", "callback_data": f"delservice_{s['name']}"}])
        buttons.append([{"text": f"📋 Шаблон: {s['name']}", "callback_data": f"template_{s['name']}"}])
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
    if not state:
        STATES.pop(str(chat_id), None)
        return send_message(chat_id, "❌ Сессия истекла. Начните заново.", reply_markup=settings_menu())
    
    name = state.get("name", "")
    price = state.get("price", 0)
    
    try:
        duration = int(dur_text.strip())
    except:
        return send_message(chat_id, "❌ Введите число минут.")
    
    master = firestore_get("masters", str(chat_id))
    if not master:
        STATES.pop(str(chat_id), None)
        return send_message(chat_id, "❌ Мастер не найден. Зарегистрируйтесь заново: /start")
    
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    services.append({"name": name, "price": price, "duration": duration})
    firestore_set("masters", str(chat_id), {"services": services})
    
    STATES.pop(str(chat_id), None)
    
    # Проверяем онбординг
    if not master.get("completed_onboarding"):
        firestore_set("masters", str(chat_id), {"completed_onboarding": True})
        send_message(chat_id, f"✅ *Отлично!*\n\nТеперь настрой часы работы:\n⚙️ Настройки → ⏰ Часы", reply_markup=master_menu())
    else:
        send_message(chat_id, f"✅ *{name}* — {price}₽, {duration}мин", reply_markup=settings_menu())

def handle_delete_service(chat_id, name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") != name]
        firestore_set("masters", str(chat_id), {"services": services})
    handle_services_settings(chat_id)

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
    except: send_message(chat_id, "❌ Формат: 09:00-20:00")

def handle_breaks_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    breaks = master.get("breaks", [])
    text = "🚫 *Перерывы:*\n" + ("\n".join([f"• {b}" for b in breaks]) if breaks else "Нет перерывов.")
    buttons = [[{"text": "➕ Добавить перерыв", "callback_data": "add_break"}], [{"text": "🔙 Назад", "callback_data": "settings_back"}]]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_add_break_prompt(chat_id):
    STATES[str(chat_id)] = {"state": "adding_break"}
    send_message(chat_id, "🚫 Введите время перерыва:\n`13:00-14:00`", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_add_break(chat_id, text):
    try:
        start, end = text.strip().split("-")
        master = firestore_get("masters", str(chat_id))
        breaks = master.get("breaks", [])
        breaks.append(f"{start.strip()}-{end.strip()}")
        firestore_set("masters", str(chat_id), {"breaks": breaks})
        STATES.pop(str(chat_id), None)
        send_message(chat_id, f"✅ Перерыв {start}-{end} добавлен.", reply_markup=settings_menu())
    except: send_message(chat_id, "❌ Формат: 13:00-14:00")

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
    if state == "setting_schedule":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_schedule_set(chat_id, text)
    if state == "adding_break":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_break(chat_id, text)
    if state == "manual_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
        return handle_manual_name(chat_id, text)
    if state == "manual_phone":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
        return handle_manual_phone(chat_id, text)
    if state == "adding_note":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
        return handle_save_note(chat_id, text)
    
    is_master = firestore_get("masters", str(chat_id))
    
    if text == "👤 Я мастер": handle_master_registration(chat_id, user_name, username)
    elif text == "👥 Я клиент":
        if not firestore_get("clients", str(chat_id)): firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
        send_message(chat_id, "👥 *Клиентский кабинет*", reply_markup=client_menu())
    elif text == "📊 Дашборд": handle_dashboard(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "📋 Мои записи": handle_client_appointments(chat_id)
    elif text == "➕ Новая запись": handle_manual_appointment_start(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "📢 Свободные окна": handle_free_slots(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "⚙️ Настройки": send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Услуги": handle_services_settings(chat_id)
    elif text == "⏰ Часы": handle_schedule_settings(chat_id)
    elif text == "🚫 Перерывы": handle_breaks_settings(chat_id)
    elif text == "🔙 В меню": STATES.pop(str(chat_id), None); send_message(chat_id, "Главное меню", reply_markup=master_menu() if is_master else client_menu())
    elif text == "🔗 Моя ссылка": handle_master_link(chat_id)
    elif text == "📅 Расписание": handle_schedule_view(chat_id)
    elif text == "👥 Клиенты": handle_clients_list(chat_id)
    else: send_message(chat_id, "Используйте меню.", reply_markup=master_menu() if is_master else client_menu())

def handle_callback(chat_id, data):
    if data == "addservice": handle_add_service_start(chat_id)
    elif data.startswith("delservice_"): handle_delete_service(chat_id, data.replace("delservice_", "", 1))
    elif data.startswith("template_"): handle_service_template(chat_id, data.replace("template_", "", 1))
    elif data == "settings_back": send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data == "add_break": handle_add_break_prompt(chat_id)
    elif data.startswith("dash_"): handle_dashboard(chat_id, period=data.replace("dash_", ""))
    elif data.startswith("cancel_"): handle_cancel_appointment(chat_id, data.replace("cancel_", "", 1))
    elif data.startswith("man_srv_"): handle_manual_service(chat_id, data.replace("man_srv_", "", 1))
    elif data.startswith("man_date_"): handle_manual_date(chat_id, data.replace("man_date_", "", 1))
    elif data.startswith("man_time_"): handle_manual_time(chat_id, data.replace("man_time_", "", 1))
    elif data.startswith("add_note_"): handle_add_client_note(chat_id, data.replace("add_note_", "", 1))
    elif data.startswith("clsrv_"): handle_client_service_select(chat_id, data.split("_")[1], data.split("_")[2])
    elif data.startswith("cldate_"): handle_client_date_select(chat_id, data.split("_")[1], data.split("_")[2])
    elif data.startswith("cltime_"): handle_client_time_select(chat_id, data.split("_")[1], data.split("_")[2], data.split("_")[3])
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