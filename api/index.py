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
    r = requests.patch(patch_url, json=body)
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

def background_worker():
    while True:
        try:
            now = datetime.now()
            reminder_1h = (now + timedelta(hours=1)).strftime('%H:%M')
            reminder_24h = (now + timedelta(hours=24)).strftime('%H:%M')
            tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')
            for date in [now.strftime('%Y-%m-%d'), tomorrow]:
                for a in firestore_query("appointments", "date", "EQUAL", date):
                    if a.get("status") != "confirmed": continue
                    if a.get("time") == reminder_1h and not a.get("reminded"):
                        send_message(int(a["master_id"]), f"⏰ *Напоминание!*\nЧерез час: {a.get('client_name')} — {a.get('service')}")
                        if "client_id" in a: send_message(int(a["client_id"]), f"⏰ *Напоминание!*\nЧерез час: {a.get('service')} в {a.get('time')}")
                        firestore_set("appointments", a["_id"], {"reminded": True})
                    if a.get("time") == reminder_24h and not a.get("reminded_24h"):
                        if "client_id" in a: send_message(int(a["client_id"]), f"📅 *Напоминание!*\nЗавтра в {a.get('time')}: {a.get('service')}")
                        firestore_set("appointments", a["_id"], {"reminded_24h": True})
            if now.hour == 10 and now.minute == 0:
                for m in firestore_query("masters", "name", "!=", ""):
                    mid = m.get("_id","")
                    if not mid: continue
                    apps = firestore_query("appointments", "master_id", "EQUAL", mid)
                    clients = {}
                    for a in apps:
                        phone = a.get("client_phone","")
                        if phone:
                            if phone not in clients: clients[phone] = {"name":a.get("client_name",""),"last":""}
                            if a.get("date","") > clients[phone]["last"]: clients[phone]["last"] = a.get("date","")
                    cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                    links = firestore_query("links", "master_id", "EQUAL", mid)
                    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
                    for phone, data in clients.items():
                        if data["last"] and data["last"] < cutoff:
                            send_message(int(mid), f"🔔 *Клиент давно не был!*\n{data['name']} ({phone})\nhttps://t.me/grafikpro_bot?start=master_{link_id}")
        except Exception as e: print(f"Worker: {e}")
        time.sleep(60)

threading.Thread(target=background_worker, daemon=True).start()

STATES = {}

# === МЕНЮ ===
def master_menu():
    return {"keyboard": [["📊 Дашборд", "🗓 Календарь"], ["📅 Расписание", "➕ Новая запись"], ["👥 Клиенты", "📢 Рассылка"], ["🔗 Моя ссылка", "⚙️ Настройки"], ["❓ Помощь"]], "resize_keyboard": True}

def client_menu():
    return {"keyboard": [["📋 Мои записи"], ["🔍 Найти мастера"], ["❓ Помощь"]], "resize_keyboard": True}

def settings_menu():
    return {"keyboard": [["💈 Услуги", "⏰ Часы"], ["🚫 Перерывы", "📍 Адрес"], ["🖼 Портфолио", "🚷 Чёрный список"], ["🔙 В меню"]], "resize_keyboard": True}

# === ПОМОЩЬ ===
def handle_help(chat_id, is_master):
    if is_master:
        text = """📖 *График.Про — помощь мастеру*
📊 *Дашборд* — доход, статистика по услугам
🗓 *Календарь* — сетка на 30 дней
📅 *Расписание* — записи с фильтрами
➕ *Новая запись* — добавить вручную
👥 *Клиенты* — список, карточки, теги
📢 *Рассылка* — сообщение всем клиентам
🔗 *Моя ссылка* — для записи клиентов
⚙️ *Настройки* — услуги, часы, портфолио"""
    else:
        text = """📖 *График.Про — помощь клиенту*
📋 *Мои записи* — все ваши записи
🔍 *Найти мастера* — поиск по телефону
💡 Запишитесь по ссылке от мастера или найдите его по номеру телефона."""
    send_message(chat_id, text)

# === СТАРТ ===
def handle_start(chat_id, user_name):
    if firestore_get("masters", str(chat_id)):
        master = firestore_get("masters", str(chat_id))
        if not master.get("completed_onboarding"):
            send_message(chat_id, f"👋 {user_name}!\n\n⚠️ Настройка не завершена.", reply_markup={"inline_keyboard": [[{"text": "🔄 Завершить настройку", "callback_data": "restart_onboarding"}]]})
        else:
            send_message(chat_id, f"👋 {user_name}!", reply_markup=master_menu())
    elif firestore_get("clients", str(chat_id)):
        send_message(chat_id, f"👋 {user_name}!", reply_markup=client_menu())
    else:
        send_message(chat_id, "👋 *График.Про*\n\nКто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})

def handle_master_registration(chat_id, user_name, username):
    firestore_set("masters", str(chat_id), {"name": user_name, "username": username, "phone": "", "services": [], "schedule": {"monday": {"start":"09:00","end":"18:00"},"tuesday": {"start":"09:00","end":"18:00"},"wednesday": {"start":"09:00","end":"18:00"},"thursday": {"start":"09:00","end":"18:00"},"friday": {"start":"09:00","end":"18:00"},"saturday": {"start":"10:00","end":"15:00"},"sunday": None}, "address": "", "completed_onboarding": False, "blacklist": [], "portfolio": [], "rating": 0, "ratings_count": 0})
    send_message(chat_id, f"✅ {user_name}, добро пожаловать!")
    start_onboarding(chat_id)

def start_onboarding(chat_id):
    send_message(chat_id, "👋 *Добро пожаловать в График.Про!*\n\n*Шаг 1 из 4:* Добавьте услуги\n_Клиенты увидят, что вы предлагаете и цены._", reply_markup={"inline_keyboard": [[{"text": "💈 Добавить услуги", "callback_data": "addservice"}], [{"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}]]})

def onboarding_step(chat_id, step):
    if step == 2:
        send_message(chat_id, "✅ *Отлично!*\n\n*Шаг 2 из 4:* Настройте часы работы\n_По дням недели, с перерывами._", reply_markup={"inline_keyboard": [[{"text": "⏰ Настроить часы", "callback_data": "onboarding_hours"}], [{"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}]]})
    elif step == 3:
        send_message(chat_id, "✅ *Часы настроены!*\n\n*Шаг 3 из 4:* Добавьте адрес\n_Клиенты будут знать, куда идти._", reply_markup={"inline_keyboard": [[{"text": "📍 Добавить адрес", "callback_data": "onboarding_address"}], [{"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}]]})
    elif step == 4:
        finish_onboarding(chat_id)

def finish_onboarding(chat_id):
    firestore_set("masters", str(chat_id), {"completed_onboarding": True})
    send_message(chat_id, "🎉 *Кабинет готов!*", reply_markup=master_menu())
    handle_master_link(chat_id)

# === ЧАСЫ ПО ДНЯМ НЕДЕЛИ ===
def handle_schedule_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    schedule = master.get("schedule", {})
    days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    day_names = ["ПН","ВТ","СР","ЧТ","ПТ","СБ","ВС"]
    text = "⏰ *Часы работы по дням:*\n"
    for i, d in enumerate(days):
        s = schedule.get(d)
        if s is None: text += f"{day_names[i]}: выходной\n"
        else: text += f"{day_names[i]}: {s.get('start','?')} – {s.get('end','?')}\n"
    text += "\nКакой день изменить? Отправьте:\n`ПН 09:00-18:00` или `ВС выходной`"
    STATES[str(chat_id)] = {"state": "setting_day_schedule"}
    send_message(chat_id, text, reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_day_schedule_set(chat_id, text):
    days_map = {"ПН":"monday","ВТ":"tuesday","СР":"wednesday","ЧТ":"thursday","ПТ":"friday","СБ":"saturday","ВС":"sunday"}
    try:
        parts = text.strip().split()
        day_code = parts[0].upper()
        day_key = days_map.get(day_code)
        if not day_key: return send_message(chat_id, "❌ Неверный день. Например: ПН 09:00-18:00")
        
        time_part = " ".join(parts[1:])
        master = firestore_get("masters", str(chat_id))
        schedule = master.get("schedule", {})
        
        if time_part.lower() == "выходной":
            schedule[day_key] = None
        else:
            start, end = time_part.split("-")
            schedule[day_key] = {"start": start.strip(), "end": end.strip()}
        
        firestore_set("masters", str(chat_id), {"schedule": schedule})
        STATES.pop(str(chat_id), None)
        send_message(chat_id, f"✅ {day_code} обновлён!", reply_markup=settings_menu())
    except Exception as e:
        send_message(chat_id, "❌ Формат: ПН 09:00-18:00 или СБ выходной")

# === ТЕГИ КЛИЕНТОВ ===
def handle_client_card(chat_id, phone):
    history = firestore_query("appointments", "client_phone", "EQUAL", phone)
    notes = firestore_get("masters", str(chat_id))
    note = notes.get("client_notes", {}).get(phone, "") if notes else ""
    tags = notes.get("client_tags", {}).get(phone, "") if notes else ""
    text = f"👤 *Клиент: {phone}*\n"
    if tags: text += f"🏷 Теги: {tags}\n"
    if note: text += f"📝 Заметка: {note}\n"
    text += "\n📋 *История:*\n"
    for h in sorted(history, key=lambda x: x.get("date", ""), reverse=True)[:10]:
        icon = {"confirmed":"🟡","completed":"✅","no_show":"❌","cancelled":"🗑"}.get(h.get("status"),"")
        text += f"{icon} {h.get('date')} — {h.get('service')}\n"
    buttons = [
        [{"text": "📝 Заметка", "callback_data": f"add_note_{phone}"}],
        [{"text": "🏷 Теги", "callback_data": f"edit_tags_{phone}"}],
        [{"text": "✏️ Редактировать", "callback_data": f"edit_client_{phone}"}],
        [{"text": "🔄 Повторить", "callback_data": f"repeat_{phone}"}],
        [{"text": "💬 Написать", "url": f"tg://resolve?phone={phone}"}],
        [{"text": "📞 Позвонить", "url": f"tel:{phone}"}],
    ]
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_edit_tags_start(chat_id, phone):
    notes = firestore_get("masters", str(chat_id)) or {}
    current_tags = notes.get("client_tags", {}).get(phone, "")
    STATES[str(chat_id)] = {"state": "editing_tags", "tags_phone": phone}
    send_message(chat_id, f"🏷 *Теги для {phone}*\nСейчас: {current_tags or 'нет'}\n\nВыберите или введите через запятую:", reply_markup={"inline_keyboard": [
        [{"text": "🏆 VIP", "callback_data": f"tag_set_{phone}_VIP"}],
        [{"text": "🔄 Постоянный", "callback_data": f"tag_set_{phone}_Постоянный"}],
        [{"text": "⚠️ Проблемный", "callback_data": f"tag_set_{phone}_Проблемный"}],
        [{"text": "🗑 Сбросить", "callback_data": f"tag_set_{phone}_"}],
        [{"text": "🔙 Назад", "callback_data": f"client_card_{phone}"}],
    ]})

def handle_tag_set(chat_id, phone, tag):
    master = firestore_get("masters", str(chat_id))
    tags = master.get("client_tags", {})
    tags[phone] = tag
    firestore_set("masters", str(chat_id), {"client_tags": tags})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Тег сохранён: {tag or 'сброшен'}", reply_markup=master_menu())

# === РЕДАКТИРОВАНИЕ КЛИЕНТА ===
def handle_edit_client_start(chat_id, phone):
    STATES[str(chat_id)] = {"state": "edit_client_name", "edit_phone": phone}
    send_message(chat_id, f"✏️ Новое имя для {phone} (или 🔙 Отмена):", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_edit_client_name(chat_id, name):
    STATES[str(chat_id)]["new_name"] = name
    STATES[str(chat_id)]["state"] = "edit_client_phone"
    send_message(chat_id, f"📞 Новый телефон (или 🔙 Отмена):")

def handle_edit_client_phone(chat_id, new_phone):
    state = STATES.pop(str(chat_id), {})
    old_phone = state.get("edit_phone", "")
    new_name = state.get("new_name", "")
    new_phone_clean = re.sub(r'[^0-9+]', '', new_phone)
    if len(new_phone_clean) < 10: return send_message(chat_id, "❌ Неверный формат телефона.")
    
    # Обновляем все записи клиента
    appointments = firestore_query("appointments", "client_phone", "EQUAL", old_phone)
    for a in appointments:
        firestore_set("appointments", a["_id"], {"client_name": new_name, "client_phone": new_phone_clean})
    
    # Обновляем заметки и теги
    master = firestore_get("masters", str(chat_id))
    notes = master.get("client_notes", {})
    if old_phone in notes:
        notes[new_phone_clean] = notes.pop(old_phone)
        firestore_set("masters", str(chat_id), {"client_notes": notes})
    tags = master.get("client_tags", {})
    if old_phone in tags:
        tags[new_phone_clean] = tags.pop(old_phone)
        firestore_set("masters", str(chat_id), {"client_tags": tags})
    
    send_message(chat_id, f"✅ Клиент обновлён: {new_name} | {new_phone_clean}", reply_markup=master_menu())

# === ОТКЛЮЧЕНИЕ УСЛУГИ ===
def handle_services_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    buttons = []
    for s in services:
        disabled = s.get("disabled", False)
        prefix = "🔴" if disabled else "🟢"
        buttons.append([{"text": f"❌ {s['name']} ({s.get('price',0)}₽)", "callback_data": f"delservice_{s['name']}"}])
        buttons.append([{"text": f"{prefix} {'Включить' if disabled else 'Отключить'}: {s['name']}", "callback_data": f"toggleservice_{s['name']}"}])
    buttons.append([{"text": "➕ Добавить", "callback_data": "addservice"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    send_message(chat_id, "💈 *Услуги:*" if services else "💈 Нет услуг.", reply_markup={"inline_keyboard": buttons})

def handle_toggle_service(chat_id, name):
    master = firestore_get("masters", str(chat_id))
    services = master.get("services", [])
    for s in services:
        if isinstance(s, dict) and s.get("name") == name:
            s["disabled"] = not s.get("disabled", False)
    firestore_set("masters", str(chat_id), {"services": services})
    handle_services_settings(chat_id)

# === СВОБОДНЫЕ ОКНА С ВЫБОРОМ ДНЯ ===
def handle_free_slots(chat_id):
    days = [("Сегодня", 0), ("Завтра", 1), ("Послезавтра", 2)]
    buttons = [[{"text": label, "callback_data": f"freeslots_{offset}"}] for label, offset in days]
    send_message(chat_id, "📅 *Свободные окна*\nВыберите день:", reply_markup={"inline_keyboard": buttons})

def handle_free_slots_day(chat_id, offset):
    date = (datetime.now() + timedelta(days=int(offset))).strftime('%Y-%m-%d')
    master = firestore_get("masters", str(chat_id))
    schedule = master.get("schedule", {})
    day_key = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][(datetime.now() + timedelta(days=int(offset))).weekday()]
    day_sched = schedule.get(day_key)
    if day_sched is None: return send_message(chat_id, "📭 В этот день выходной.")
    
    start_h = int(day_sched["start"].split(":")[0])
    end_h = int(day_sched["end"].split(":")[0])
    busy = {a.get("time") for a in firestore_query("appointments", "master_id", "EQUAL", str(chat_id)) if a.get("date") == date and a.get("status") not in ["cancelled"]}
    free = [f"{h}:00" for h in range(start_h, end_h + 1) if f"{h}:00" not in busy]
    if not free: return send_message(chat_id, "📭 Всё занято!")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    send_message(chat_id, f"🟢 *Свободные окна на {date}:*\n{', '.join(free)}\n\nhttps://t.me/grafikpro_bot?start=master_{link_id}")

# === ЭКСПОРТ С ВЫБОРОМ ПЕРИОДА ===
def handle_export_excel(chat_id):
    send_message(chat_id, "📥 *Экспорт отчёта*\nВыберите период:", reply_markup={"inline_keyboard": [
        [{"text": "📅 За неделю", "callback_data": "export_week"}],
        [{"text": "📅 За месяц", "callback_data": "export_month"}],
        [{"text": "📅 Всё время", "callback_data": "export_all"}],
    ]})

def handle_export_period(chat_id, period):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "Нет данных.")
    
    if period == "week":
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        filtered = [a for a in appointments if a.get("date","") >= week_ago]
    elif period == "month":
        month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        filtered = [a for a in appointments if a.get("date","") >= month_ago]
    else:
        filtered = appointments
    
    csv = "Дата;Время;Клиент;Телефон;Услуга;Комментарий;Статус\n"
    for a in sorted(filtered, key=lambda x: x.get("date","")):
        csv += f"{a.get('date','')};{a.get('time','')};{a.get('client_name','')};{a.get('client_phone','')};{a.get('service','')};{a.get('comment','')};{a.get('status','')}\n"
    
    import io
    buf = io.BytesIO()
    buf.write(csv.encode('utf-8-sig'))
    buf.seek(0)
    send_document(chat_id, buf.read(), f"otchet_{period}.csv")

# === УДАЛЕНИЕ С ПРИЧИНОЙ ===
def handle_delete_prompt(chat_id, appt_id):
    STATES[str(chat_id)] = {"state": "delete_reason", "delete_appt_id": appt_id}
    send_message(chat_id, "⚠️ *Причина отмены?*", reply_markup={"inline_keyboard": [
        [{"text": "📅 Перенос", "callback_data": f"delreason_{appt_id}_Перенос"}],
        [{"text": "🚫 Отказ клиента", "callback_data": f"delreason_{appt_id}_Отказ клиента"}],
        [{"text": "🔄 Изменились планы", "callback_data": f"delreason_{appt_id}_Изменились планы"}],
        [{"text": "🗑 Без причины", "callback_data": f"delete_{appt_id}"}],
        [{"text": "🔙 Отмена", "callback_data": "filter_all"}],
    ]})

def handle_delete_with_reason(chat_id, appt_id, reason):
    appt = firestore_get("appointments", appt_id)
    if appt and appt.get("client_id"): 
        msg = f"❌ *Запись отменена*\n{appt.get('service')}\n{appt.get('date')} в {appt.get('time')}"
        if reason: msg += f"\n\nПричина: {reason}"
        send_message(int(appt["client_id"]), msg)
    firestore_set("appointments", appt_id, {"status": "cancelled", "cancel_reason": reason})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"🗑 Запись удалена\nПричина: {reason}", reply_markup=master_menu())

# === ОБРАБОТКА ТЕКСТА ===
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
    if state == "adding_service_description":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_service_description(chat_id, text)
    if state == "photo_caption":
        return handle_photo_caption(chat_id, text)
    if state == "client_entering_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.")
        return handle_client_name(chat_id, text)
    if state == "client_entering_phone":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.")
        return handle_client_phone(chat_id, text)
    if state == "setting_day_schedule":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_day_schedule_set(chat_id, text)
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
    if state == "edit_client_name":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
        return handle_edit_client_name(chat_id, text)
    if state == "edit_client_phone":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
        return handle_edit_client_phone(chat_id, text)
    if state == "setting_address":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_address_set(chat_id, text)
    if state == "adding_to_blacklist":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=settings_menu())
        return handle_add_to_blacklist(chat_id, text)
    if state == "finding_master":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=client_menu())
        return handle_find_master(chat_id, text)
    if state == "broadcast":
        if text == "🔙 Отмена": STATES.pop(str(chat_id), None); return send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
        return handle_broadcast_preview(chat_id, text)
    if state == "client_comment":
        return handle_client_comment(chat_id, text)
    
    is_master = firestore_get("masters", str(chat_id))
    if text == "👤 Я мастер": handle_master_registration(chat_id, user_name, username)
    elif text == "👥 Я клиент":
        if not firestore_get("clients", str(chat_id)): firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
        send_message(chat_id, "👥 *Клиентский кабинет*", reply_markup=client_menu())
    elif text == "📊 Дашборд": handle_dashboard(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "🗓 Календарь": handle_calendar(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "📋 Мои записи": handle_client_appointments(chat_id)
    elif text == "🔍 Найти мастера": handle_find_master_start(chat_id)
    elif text == "➕ Новая запись": handle_manual_appointment_start(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "📢 Рассылка": handle_broadcast_start(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    elif text == "⚙️ Настройки": send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Услуги": handle_services_settings(chat_id)
    elif text == "⏰ Часы": handle_schedule_settings(chat_id)
    elif text == "🚫 Перерывы": handle_breaks_settings(chat_id)
    elif text == "📍 Адрес": handle_address_settings(chat_id)
    elif text == "🖼 Портфолио": handle_portfolio_settings(chat_id)
    elif text == "🚷 Чёрный список": handle_blacklist_settings(chat_id)
    elif text == "❓ Помощь": handle_help(chat_id, is_master)
    elif text == "🔙 В меню": STATES.pop(str(chat_id), None); send_message(chat_id, "Главное меню", reply_markup=master_menu() if is_master else client_menu())
    elif text == "🔗 Моя ссылка": handle_master_link(chat_id)
    elif text == "📅 Расписание": handle_schedule_view(chat_id)
    elif text == "👥 Клиенты": handle_clients_list(chat_id)
    elif text == "📢 Свободные окна": handle_free_slots(chat_id) if is_master else send_message(chat_id, "Только для мастера.")
    else: send_message(chat_id, "Используйте меню.", reply_markup=master_menu() if is_master else client_menu())

# === CALLBACK ===
def handle_callback(chat_id, data):
    if data == "addservice": handle_add_service_start(chat_id)
    elif data.startswith("delservice_"): handle_delete_service(chat_id, data.replace("delservice_", "", 1))
    elif data.startswith("toggleservice_"): handle_toggle_service(chat_id, data.replace("toggleservice_", "", 1))
    elif data == "settings_back": send_message(chat_id, "⚙️ Настройки", reply_markup=settings_menu())
    elif data == "add_break": handle_add_break_prompt(chat_id)
    elif data == "add_to_blacklist": handle_add_to_blacklist_start(chat_id)
    elif data.startswith("unblock_"): handle_unblock(chat_id, data.replace("unblock_", "", 1))
    elif data == "clear_portfolio": handle_clear_portfolio(chat_id)
    elif data.startswith("del_photo_"): handle_delete_photo(chat_id, int(data.replace("del_photo_", "")))
    elif data == "export_excel": handle_export_excel(chat_id)
    elif data.startswith("export_"): handle_export_period(chat_id, data.replace("export_", ""))
    elif data == "broadcast_send": handle_broadcast_send(chat_id)
    elif data == "broadcast_cancel": STATES.pop(str(chat_id), None); send_message(chat_id, "❌ Отменено.", reply_markup=master_menu())
    elif data.startswith("dash_"): handle_dashboard(chat_id, period=data.replace("dash_", ""))
    elif data.startswith("cancel_"): handle_cancel_appointment(chat_id, data.replace("cancel_", "", 1))
    elif data.startswith("complete_"): handle_complete_appointment(chat_id, data.replace("complete_", "", 1))
    elif data.startswith("noshow_"): handle_noshow_appointment(chat_id, data.replace("noshow_", "", 1))
    elif data.startswith("delete_prompt_"): handle_delete_prompt(chat_id, data.replace("delete_prompt_", "", 1))
    elif data.startswith("delreason_"):
        parts = data.replace("delreason_", "").split("_", 1)
        handle_delete_with_reason(chat_id, parts[0], parts[1])
    elif data.startswith("delete_"): handle_delete_appointment(chat_id, data.replace("delete_", "", 1))
    elif data.startswith("reschedule_"): handle_reschedule_start(chat_id, data.replace("reschedule_", "", 1))
    elif data.startswith("res_date_"):
        parts = data.replace("res_date_", "").split("_", 1)
        handle_reschedule_date(chat_id, parts[0], parts[1])
    elif data.startswith("res_time_"):
        parts = data.replace("res_time_", "").split("_", 2)
        handle_reschedule_time(chat_id, parts[0], parts[1], parts[2])
    elif data.startswith("cl_reschedule_"): handle_client_reschedule_start(chat_id, data.replace("cl_reschedule_", "", 1))
    elif data.startswith("cl_res_date_"):
        parts = data.replace("cl_res_date_", "").split("_", 1)
        handle_client_reschedule_date(chat_id, parts[0], parts[1])
    elif data.startswith("cl_res_time_"):
        parts = data.replace("cl_res_time_", "").split("_", 2)
        handle_client_reschedule_time(chat_id, parts[0], parts[1], parts[2])
    elif data.startswith("rate_"):
        parts = data.replace("rate_", "").split("_", 1)
        handle_rate_master(chat_id, parts[0], int(parts[1]))
    elif data.startswith("man_srv_"): handle_manual_service(chat_id, data.replace("man_srv_", "", 1))
    elif data.startswith("man_date_"): handle_manual_date(chat_id, data.replace("man_date_", "", 1))
    elif data.startswith("man_time_"): handle_manual_time(chat_id, data.replace("man_time_", "", 1))
    elif data.startswith("add_note_"): handle_add_client_note(chat_id, data.replace("add_note_", "", 1))
    elif data.startswith("edit_tags_"): handle_edit_tags_start(chat_id, data.replace("edit_tags_", "", 1))
    elif data.startswith("tag_set_"):
        parts = data.replace("tag_set_", "").split("_", 1)
        handle_tag_set(chat_id, parts[0], parts[1])
    elif data.startswith("edit_client_"): handle_edit_client_start(chat_id, data.replace("edit_client_", "", 1))
    elif data.startswith("client_card_"): handle_client_card(chat_id, data.replace("client_card_", "", 1))
    elif data.startswith("repeat_"): handle_repeat_appointment(chat_id, data.replace("repeat_", "", 1))
    elif data.startswith("freeslots_"): handle_free_slots_day(chat_id, data.replace("freeslots_", ""))
    elif data.startswith("clsrv_"): handle_client_service_select(chat_id, data.split("_")[1], data.split("_")[2])
    elif data.startswith("cldate_"): handle_client_date_select(chat_id, data.split("_")[1], data.split("_")[2])
    elif data.startswith("cltime_"): handle_client_time_select(chat_id, data.split("_")[1], data.split("_")[2], data.split("_")[3])
    elif data.startswith("filter_"): handle_schedule_view(chat_id, filter_mode=data.replace("filter_", ""))
    elif data == "onboarding_skip": finish_onboarding(chat_id)
    elif data == "onboarding_hours": handle_schedule_settings(chat_id)
    elif data == "onboarding_address": handle_address_settings(chat_id)
    elif data == "restart_onboarding": start_onboarding(chat_id)
    elif data == "ignore": pass

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
def get_smart_slots(master, date, service_name):
    schedule = master.get("schedule", {})
    day_key = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][datetime.strptime(date, '%Y-%m-%d').weekday()]
    day_sched = schedule.get(day_key)
    if day_sched is None: return []
    start_h = int(day_sched["start"].split(":")[0])
    end_h = int(day_sched["end"].split(":")[0])
    breaks = master.get("breaks", [])
    services = master.get("services", [])
    svc = next((s for s in services if isinstance(s, dict) and s.get("name") == service_name and not s.get("disabled")), None)
    if not svc: return []
    duration = svc.get("duration", 60)
    busy_intervals = []
    for a in firestore_query("appointments", "master_id", "EQUAL", str(master.get("chat_id", ""))):
        if a.get("date") == date and a.get("status") not in ["cancelled"]:
            a_svc = next((s for s in services if isinstance(s, dict) and s.get("name") == a.get("service")), None)
            a_dur = a_svc.get("duration", 60) if a_svc else 60
            busy_start = int(a["time"].split(":")[0]) * 60
            busy_intervals.append((busy_start, busy_start + a_dur))
    for b in breaks:
        try: busy_intervals.append((int(b.split("-")[0].split(":")[0])*60, int(b.split("-")[1].split(":")[0])*60))
        except: pass
    slots = []
    t = start_h * 60
    end_t = end_h * 60
    while t + duration <= end_t:
        time_str = f"{t // 60:02d}:{t % 60:02d}"
        busy = any(not (t + duration <= bs or t >= be) for (bs, be) in busy_intervals)
        slots.append((time_str, busy))
        t += 30
    return slots

def handle_calendar(chat_id):
    send_message(chat_id, f"📅 *Календарь*\n\n[Открыть календарь](https://grafikpro-final-rho.vercel.app/api/calendar?m={chat_id})", reply_markup=master_menu())

def handle_dashboard(chat_id, period="today"):
    today = datetime.now().strftime('%Y-%m-%d')
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    master = firestore_get("masters", str(chat_id)) or {}
    services = master.get("services", [])
    if period == "week":
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        filtered = [a for a in appointments if a.get("date", "") >= week_ago and a.get("status") == "completed"]
        prev_filtered = [a for a in appointments if (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d') <= a.get("date", "") < week_ago and a.get("status") == "completed"]
        label = "📊 *Доход за неделю*"
    elif period == "month":
        month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        filtered = [a for a in appointments if a.get("date", "") >= month_ago and a.get("status") == "completed"]
        prev_filtered = [a for a in appointments if (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d') <= a.get("date", "") < month_ago and a.get("status") == "completed"]
        label = "📊 *Доход за месяц*"
    else:
        filtered = [a for a in appointments if a.get("date") == today and a.get("status") != "cancelled"]
        prev_filtered = []
        label = "📊 *Дашборд на сегодня*"
    def calc_total(appts):
        total = 0
        for a in appts:
            svc = a.get("service", "")
            for s in services:
                if isinstance(s, dict) and s.get("name") == svc: total += s.get("price", 0)
        return total
    total = calc_total(filtered)
    prev_total = calc_total(prev_filtered)
    text = f"{label}\n\n📅 Записей: {len(filtered)}\n💰 Доход: {total}₽"
    if prev_total > 0:
        change = int((total - prev_total) / prev_total * 100)
        text += f"\n\n{'📈' if change > 0 else '📉'} {change:+d}% к прошлому периоду"
    svc_stats = {}
    for a in filtered:
        svc = a.get("service", "")
        if svc not in svc_stats: svc_stats[svc] = {"count": 0, "total": 0}
        for s in services:
            if isinstance(s, dict) and s.get("name") == svc: svc_stats[svc]["total"] += s.get("price", 0)
        svc_stats[svc]["count"] += 1
    if svc_stats:
        text += "\n\n💈 *По услугам:*\n"
        for svc, data in sorted(svc_stats.items(), key=lambda x: x[1]["total"], reverse=True):
            text += f"• {svc}: {data['count']} зап. — {data['total']}₽\n"
    send_message(chat_id, text, reply_markup={"inline_keyboard": [[{"text": "Сегодня", "callback_data": "dash_today"}, {"text": "Неделя", "callback_data": "dash_week"}, {"text": "Месяц", "callback_data": "dash_month"}], [{"text": "📥 Скачать отчёт", "callback_data": "export_excel"}]]})

def handle_master_link(chat_id):
    if not firestore_get("masters", str(chat_id)): return send_message(chat_id, "Сначала зарегистрируйтесь.")
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": str(chat_id)})
    send_message(chat_id, f"🔗 *Твоя ссылка:*\n\n📨 [Открыть в Telegram](tg://resolve?domain=grafikpro_bot&start=master_{link_id})\n\n`https://t.me/grafikpro_bot?start=master_{link_id}`")

def handle_client_start(chat_id, link_id):
    if not firestore_get("clients", str(chat_id)): firestore_set("clients", str(chat_id), {"created_at": datetime.now().isoformat()})
    link = firestore_get("links", link_id)
    if not link: return send_message(chat_id, "❌ Ссылка недействительна.")
    master = firestore_get("masters", link["master_id"])
    if not master: return send_message(chat_id, "❌ Мастер не найден.")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") and not s.get("disabled")]
    if not services: return send_message(chat_id, "❌ Нет доступных услуг.")
    addr = master.get("address", "")
    rating = master.get("rating", 0)
    text = f"👤 *{master.get('name')}*"
    if rating: text += f"\n⭐ {rating}/5 ({master.get('ratings_count', 0)} оценок)"
    if addr: text += f"\n📍 {addr}"
    text += "\n\nВыберите услугу:"
    buttons = [[{"text": f"{s['name']} — {s['price']}₽ ({s.get('duration',60)}мин)", "callback_data": f"clsrv_{link_id}_{s['name']}"}] for s in services]
    STATES[str(chat_id)] = {"link_id": link_id, "master_name": master.get("name"), "master_addr": addr, "master_id": link["master_id"]}
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
    service = STATES.get(str(chat_id), {}).get("service", "")
    slots = get_smart_slots(master, date, service)
    if not slots: return send_message(chat_id, "📭 В этот день нет свободных слотов (выходной или всё занято).")
    STATES[str(chat_id)]["date"] = date
    buttons = [[{"text": f"{'🟢' if not b else '❌'} {t}", "callback_data": f"cltime_{link_id}_{date}_{t}" if not b else "ignore"}] for (t, b) in slots]
    send_message(chat_id, f"⏰ *Выберите время:*\n{date}", reply_markup={"inline_keyboard": buttons})

def handle_client_time_select(chat_id, link_id, date, time):
    STATES[str(chat_id)] = STATES.get(str(chat_id), {})
    STATES[str(chat_id)]["time"] = time
    STATES[str(chat_id)]["state"] = "client_comment"
    send_message(chat_id, "💬 *Комментарий к записи:* (можно пропустить)", reply_markup={"keyboard": [["⏩ Пропустить"], ["🔙 Отмена"]], "resize_keyboard": True})

def handle_client_comment(chat_id, comment):
    if comment == "⏩ Пропустить": comment = ""
    STATES[str(chat_id)]["comment"] = comment
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
    if phone_clean in master.get("blacklist", []): return send_message(chat_id, "❌ Вы не можете записаться.")
    addr = master.get("address", "")
    firestore_add("appointments", {"master_id": link["master_id"], "client_id": str(chat_id), "client_name": state["client_name"], "client_phone": phone_clean, "service": state["service"], "date": state["date"], "time": state["time"], "comment": state.get("comment", ""), "status": "confirmed"})
    ics = f"BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nDTSTART:{state['date'].replace('-','')}T{state['time'].replace(':','')}00\nSUMMARY:{master.get('name')} - {state.get('service')}\nEND:VEVENT\nEND:VCALENDAR"
    send_document(chat_id, ics.encode(), "appointment.ics")
    text = f"✅ *Запись подтверждена!*\n\n{master.get('name')}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}"
    if addr: text += f"\n📍 {addr}"
    if state.get("comment"): text += f"\n💬 {state.get('comment')}"
    send_message(chat_id, text + f"\n\n{state.get('client_name')} | {phone_clean}", reply_markup=client_menu())
    send_message(int(link["master_id"]), f"🔔 *Новая запись!*\n\n{state.get('client_name')}\n{phone_clean}\n{state.get('service')}\n{state.get('date')} в {state.get('time')}" + (f"\n💬 {state.get('comment')}" if state.get("comment") else ""))

def handle_schedule_view(chat_id, filter_mode="all"):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "📭 Записей пока нет.")
    today = datetime.now().strftime('%Y-%m-%d')
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    week_end = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
    if filter_mode == "today": filtered = [a for a in appointments if a.get("date") == today]
    elif filter_mode == "tomorrow": filtered = [a for a in appointments if a.get("date") == tomorrow]
    elif filter_mode == "week": filtered = [a for a in appointments if today <= a.get("date", "") <= week_end]
    else: filtered = appointments
    filtered.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    text = "📅 *Расписание:*\n"
    buttons = []
    for a in filtered:
        if a.get("status") == "cancelled": continue
        date_str = a.get("date","")
        if date_str == today: date_str = "🟢 Сегодня"
        elif date_str == tomorrow: date_str = "🔵 Завтра"
        status_icon = {"confirmed": "🟡", "completed": "✅", "no_show": "❌"}.get(a.get("status"), "")
        text += f"\n{status_icon} {date_str} {a.get('time')} — {a.get('service')}\n  {a.get('client_name','?')} | {a.get('client_phone','?')}"
        if a.get("comment"): text += f"\n  💬 {a.get('comment')}"
        if a.get("status") == "confirmed":
            buttons.append([{"text": f"✅ Вып: {a.get('date')} {a.get('time')}", "callback_data": f"complete_{a['_id']}"}])
            buttons.append([{"text": f"❌ Неявка: {a.get('date')} {a.get('time')}", "callback_data": f"noshow_{a['_id']}"}])
        buttons.append([{"text": f"🔄 Перенести: {a.get('date')} {a.get('time')}", "callback_data": f"reschedule_{a['_id']}"}])
        buttons.append([{"text": f"🗑 Удалить: {a.get('date')} {a.get('time')}", "callback_data": f"delete_prompt_{a['_id']}"}])
    buttons.append([{"text": f, "callback_data": f"filter_{f}"} for f in ["Сегодня","Завтра","Неделя","Всё"]])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_complete_appointment(chat_id, appt_id):
    firestore_set("appointments", appt_id, {"status": "completed"})
    appt = firestore_get("appointments", appt_id)
    if appt and appt.get("client_id"):
        send_message(int(appt["client_id"]), f"⭐ *Оцените визит!*\n\nКак прошла услуга «{appt.get('service')}»?", reply_markup={"inline_keyboard": [[{"text": f"{'⭐'*i}", "callback_data": f"rate_{appt['master_id']}_{i}"}] for i in range(1,6)]})
    send_message(chat_id, "✅ Выполнено!", reply_markup=master_menu())

def handle_noshow_appointment(chat_id, appt_id):
    firestore_set("appointments", appt_id, {"status": "no_show"})
    send_message(chat_id, "❌ Неявка.", reply_markup=master_menu())

def handle_delete_appointment(chat_id, appt_id):
    appt = firestore_get("appointments", appt_id)
    if appt and appt.get("client_id"): send_message(int(appt["client_id"]), f"❌ *Запись отменена*\n{appt.get('service')}\n{appt.get('date')} в {appt.get('time')}")
    firestore_set("appointments", appt_id, {"status": "cancelled"})
    send_message(chat_id, "🗑 Запись удалена.", reply_markup=master_menu())

def handle_reschedule_start(chat_id, appt_id):
    appt = firestore_get("appointments", appt_id)
    if not appt: return send_message(chat_id, "Запись не найдена.")
    STATES[str(chat_id)] = {"state": "reschedule_date", "appt_id": appt_id}
    buttons = [[{"text": (datetime.now() + timedelta(days=i)).strftime('%d.%m'), "callback_data": f"res_date_{appt_id}_{(datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d')}"}] for i in range(14)]
    send_message(chat_id, "📅 Новая дата:", reply_markup={"inline_keyboard": buttons})

def handle_reschedule_date(chat_id, appt_id, date):
    STATES[str(chat_id)] = {"state": "reschedule_time", "appt_id": appt_id, "new_date": date}
    appt = firestore_get("appointments", appt_id)
    master = firestore_get("masters", appt["master_id"])
    slots = get_smart_slots(master, date, appt.get("service", ""))
    buttons = [[{"text": f"{'🟢' if not b else '❌'} {t}", "callback_data": f"res_time_{appt_id}_{date}_{t}" if not b else "ignore"}] for (t, b) in slots]
    send_message(chat_id, f"⏰ Время на {date}:", reply_markup={"inline_keyboard": buttons})

def handle_reschedule_time(chat_id, appt_id, date, time):
    firestore_set("appointments", appt_id, {"date": date, "time": time, "reminded": False, "reminded_24h": False})
    appt = firestore_get("appointments", appt_id)
    if appt.get("client_id"): send_message(int(appt["client_id"]), f"🔄 *Перенесено!*\n{appt.get('service')}\nНовое: {date} в {time}")
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Перенесено на {date} {time}", reply_markup=master_menu())

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
        if a.get("comment"): text += f"\n  💬 {a.get('comment')}"
        if a.get("status") == "confirmed":
            buttons.append([{"text": f"🔄 Перенести: {a.get('date')} {a.get('time')}", "callback_data": f"cl_reschedule_{a['_id']}"}])
            buttons.append([{"text": f"❌ Отменить: {a.get('date')} {a.get('time')}", "callback_data": f"cancel_{a['_id']}"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_client_reschedule_start(chat_id, appt_id):
    appt = firestore_get("appointments", appt_id)
    if not appt or appt.get("client_id") != str(chat_id): return send_message(chat_id, "❌ Ошибка.")
    STATES[str(chat_id)] = {"state": "client_reschedule_date", "appt_id": appt_id}
    send_message(chat_id, "🔄 *Перенос записи*\nНовая дата:", reply_markup={"inline_keyboard": [[{"text": (datetime.now() + timedelta(days=i+1)).strftime('%d.%m (%a)'), "callback_data": f"cl_res_date_{appt_id}_{(datetime.now() + timedelta(days=i+1)).strftime('%Y-%m-%d')}"}] for i in range(7)]})

def handle_client_reschedule_date(chat_id, appt_id, date):
    STATES[str(chat_id)] = {"state": "client_reschedule_time", "appt_id": appt_id, "new_date": date}
    appt = firestore_get("appointments", appt_id)
    master = firestore_get("masters", appt["master_id"])
    slots = get_smart_slots(master, date, appt.get("service", ""))
    buttons = [[{"text": f"{'🟢' if not b else '❌'} {t}", "callback_data": f"cl_res_time_{appt_id}_{date}_{t}" if not b else "ignore"}] for (t, b) in slots]
    send_message(chat_id, f"⏰ Новое время на {date}:", reply_markup={"inline_keyboard": buttons})

def handle_client_reschedule_time(chat_id, appt_id, date, time):
    firestore_set("appointments", appt_id, {"date": date, "time": time, "reminded": False, "reminded_24h": False})
    appt = firestore_get("appointments", appt_id)
    send_message(int(appt["master_id"]), f"🔄 *Клиент перенёс запись!*\n{appt.get('client_name')}\n{appt.get('service')}\nНовое: {date} в {time}")
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ Перенесено на {date} {time}", reply_markup=client_menu())

def handle_cancel_appointment(chat_id, appt_id):
    appt = firestore_get("appointments", appt_id)
    if not appt or appt.get("client_id") != str(chat_id): return send_message(chat_id, "❌ Ошибка.")
    if appt.get("master_id"): send_message(int(appt["master_id"]), f"❌ *Отмена!*\n{appt.get('client_name')} отменил {appt.get('service')} {appt.get('date')} в {appt.get('time')}")
    firestore_set("appointments", appt_id, {"status": "cancelled"})
    send_message(chat_id, "✅ Отменено.", reply_markup=client_menu())

def handle_clients_list(chat_id):
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments: return send_message(chat_id, "👥 Пока нет клиентов.")
    clients = {}
    for a in appointments:
        cid = a.get("client_phone", "")
        if cid not in clients: clients[cid] = {"name": a.get("client_name", "?"), "phone": cid, "history": []}
        clients[cid]["history"].append(f"{a.get('service')} ({a.get('date')})")
    text = "👥 *Клиенты:*\n"
    buttons = []
    for cid, data in list(clients.items())[:10]:
        text += f"\n• *{data['name']}* | {data['phone']}\n  {', '.join(data['history'][-3:])}"
        buttons.append([{"text": f"👤 {data['name']}", "callback_data": f"client_card_{data['phone']}"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_repeat_appointment(chat_id, client_phone):
    links = firestore_query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    history = firestore_query("appointments", "client_phone", "EQUAL", client_phone)
    last_svc = history[-1].get("service", "услугу") if history else "услугу"
    send_message(chat_id, f"👋 Запишитесь снова на «{last_svc}»!\n\ntg://resolve?domain=grafikpro_bot&start=master_{link_id}")

def handle_find_master_start(chat_id):
    STATES[str(chat_id)] = {"state": "finding_master"}
    send_message(chat_id, "🔍 Введите номер телефона мастера:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_find_master(chat_id, phone):
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    masters = firestore_query("masters", "phone", "EQUAL", phone_clean)
    if not masters:
        STATES.pop(str(chat_id), None)
        return send_message(chat_id, "❌ Мастер не найден.", reply_markup=client_menu())
    master = masters[0]
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") and not s.get("disabled")]
    addr = master.get("address", "Не указан")
    rating = master.get("rating", 0)
    text = f"👤 *{master.get('name')}*\n📍 {addr}"
    if rating: text += f"\n⭐ {rating}/5 ({master.get('ratings_count', 0)} оценок)"
    text += "\n\n💈 *Услуги:*\n" + "\n".join([f"• {s['name']} — {s['price']}₽" for s in services]) if services else ""
    links = firestore_query("links", "master_id", "EQUAL", master.get("_id", ""))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: firestore_set("links", link_id, {"master_id": master.get("_id", "")})
    STATES.pop(str(chat_id), None)
    buttons = [[{"text": "📝 Записаться", "callback_data": f"clsrv_{link_id}_{services[0]['name']}"}]] if services else []
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_rate_master(chat_id, master_id, rating):
    master = firestore_get("masters", master_id)
    if not master: return
    current_rating = master.get("rating", 0)
    ratings_count = master.get("ratings_count", 0)
    new_rating = int((current_rating * ratings_count + rating) / (ratings_count + 1))
    firestore_set("masters", master_id, {"rating": new_rating, "ratings_count": ratings_count + 1})
    send_message(chat_id, f"⭐ Спасибо за оценку!", reply_markup=client_menu())

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
    STATES[str(chat_id)] = {"state": "adding_service_description", "name": state["name"], "price": state["price"], "duration": duration}
    send_message(chat_id, f"📝 Описание (можно пропустить):", reply_markup={"keyboard": [["⏩ Пропустить"]], "resize_keyboard": True})

def handle_add_service_description(chat_id, desc):
    state = STATES.get(str(chat_id), {})
    if desc == "⏩ Пропустить": desc = ""
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "❌ Зарегистрируйтесь: /start")
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    services.append({"name": state["name"], "price": state["price"], "duration": state["duration"], "description": desc, "disabled": False})
    firestore_set("masters", str(chat_id), {"services": services})
    STATES.pop(str(chat_id), None)
    if not master.get("completed_onboarding"):
        firestore_set("masters", str(chat_id), {"completed_onboarding": True})
        onboarding_step(chat_id, 2)
    else:
        send_message(chat_id, f"✅ *{state['name']}* — {state['price']}₽, {state['duration']}мин", reply_markup=settings_menu())

def handle_delete_service(chat_id, name):
    master = firestore_get("masters", str(chat_id))
    if master:
        services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") != name]
        firestore_set("masters", str(chat_id), {"services": services})
    handle_services_settings(chat_id)

def handle_breaks_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    breaks = master.get("breaks", [])
    text = "🚫 *Перерывы:*\n" + ("\n".join([f"• {b}" for b in breaks]) if breaks else "Нет перерывов.")
    send_message(chat_id, text, reply_markup={"inline_keyboard": [[{"text": "➕ Добавить", "callback_data": "add_break"}], [{"text": "🔙 Назад", "callback_data": "settings_back"}]]})

def handle_add_break_prompt(chat_id):
    STATES[str(chat_id)] = {"state": "adding_break"}
    send_message(chat_id, "🚫 Время перерыва:\n`13:00-14:00`", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

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

def handle_blacklist_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    blacklist = master.get("blacklist", [])
    text = "🚷 *Чёрный список:*\n" + ("\n".join([f"• {b}" for b in blacklist]) if blacklist else "Пуст.")
    buttons = [[{"text": f"🗑 {b}", "callback_data": f"unblock_{b}"}] for b in blacklist]
    buttons.append([{"text": "➕ Добавить", "callback_data": "add_to_blacklist"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_add_to_blacklist_start(chat_id):
    STATES[str(chat_id)] = {"state": "adding_to_blacklist"}
    send_message(chat_id, "🚷 Введите номер телефона:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_add_to_blacklist(chat_id, phone):
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    master = firestore_get("masters", str(chat_id))
    blacklist = master.get("blacklist", [])
    if phone_clean not in blacklist:
        blacklist.append(phone_clean)
        firestore_set("masters", str(chat_id), {"blacklist": blacklist})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, f"✅ {phone_clean} добавлен.", reply_markup=settings_menu())

def handle_unblock(chat_id, phone):
    master = firestore_get("masters", str(chat_id))
    blacklist = master.get("blacklist", [])
    blacklist = [b for b in blacklist if b != phone]
    firestore_set("masters", str(chat_id), {"blacklist": blacklist})
    handle_blacklist_settings(chat_id)

def handle_manual_appointment_start(chat_id):
    STATES[str(chat_id)] = {"state": "manual_name"}
    send_message(chat_id, "📝 *Новая запись*\nИмя клиента:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_manual_name(chat_id, name):
    STATES[str(chat_id)] = {"state": "manual_phone", "client_name": name}
    send_message(chat_id, "📞 Телефон клиента:")

def handle_manual_phone(chat_id, phone):
    phone_clean = re.sub(r'[^0-9+]', '', phone)
    if len(phone_clean) < 10: return send_message(chat_id, "❌ Неверный формат.")
    STATE = STATES[str(chat_id)]
    STATE["state"] = "manual_service"
    STATE["client_phone"] = phone_clean
    history = firestore_query("appointments", "client_phone", "EQUAL", phone_clean)
    notes = firestore_get("masters", str(chat_id))
    note = notes.get("client_notes", {}).get(phone_clean, "") if notes else ""
    if history or note:
        text = "📋 *История:*\n"
        for h in history[-5:]: text += f"• {h.get('date')} — {h.get('service')}\n"
        if note: text += f"\n📝 Заметка: {note}"
        send_message(chat_id, text)
    master = firestore_get("masters", str(chat_id))
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not services: return send_message(chat_id, "❌ Сначала добавьте услуги.")
    buttons = [[{"text": f"{s['name']} ({s['price']}₽)", "callback_data": f"man_srv_{s['name']}"}] for s in services]
    buttons.append([{"text": "📝 Заметка", "callback_data": f"add_note_{phone_clean}"}])
    send_message(chat_id, "💈 Услуга:", reply_markup={"inline_keyboard": buttons})

def handle_add_client_note(chat_id, phone):
    STATES[str(chat_id)] = {"state": "adding_note", "note_phone": phone}
    send_message(chat_id, "📝 Заметка о клиенте:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_save_note(chat_id, text):
    phone = STATES.get(str(chat_id), {}).get("note_phone", "")
    master = firestore_get("masters", str(chat_id))
    notes = master.get("client_notes", {})
    notes[phone] = text
    firestore_set("masters", str(chat_id), {"client_notes": notes})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, "✅ Заметка сохранена!", reply_markup=master_menu())

def handle_manual_service(chat_id, service_name):
    STATES[str(chat_id)]["service"] = service_name
    STATES[str(chat_id)]["state"] = "manual_date"
    buttons = [[{"text": ("Сегодня" if i==0 else (datetime.now()+timedelta(days=i)).strftime('%d.%m')), "callback_data": f"man_date_{(datetime.now()+timedelta(days=i)).strftime('%Y-%m-%d')}"}] for i in range(14)]
    send_message(chat_id, "📅 Дата:", reply_markup={"inline_keyboard": buttons})

def handle_manual_date(chat_id, date):
    STATES[str(chat_id)]["date"] = date
    STATES[str(chat_id)]["state"] = "manual_time"
    master = firestore_get("masters", str(chat_id))
    svc_name = STATES[str(chat_id)].get("service", "")
    slots = get_smart_slots(master, date, svc_name)
    buttons = [[{"text": f"{'🟢' if not b else '❌'} {t}", "callback_data": f"man_time_{t}" if not b else "ignore"}] for (t, b) in slots]
    send_message(chat_id, f"⏰ Время:", reply_markup={"inline_keyboard": buttons})

def handle_manual_time(chat_id, time):
    state = STATES.pop(str(chat_id), {})
    firestore_add("appointments", {"master_id": str(chat_id), "client_name": state.get("client_name",""), "client_phone": state.get("client_phone",""), "service": state.get("service",""), "date": state.get("date",""), "time": time, "status": "confirmed"})
    send_message(chat_id, f"✅ {state.get('client_name')}\n{state.get('service')}\n{state.get('date')} в {time}", reply_markup=master_menu())

def handle_portfolio_settings(chat_id):
    master = firestore_get("masters", str(chat_id))
    if not master: return send_message(chat_id, "Сначала зарегистрируйтесь.")
    portfolio = master.get("portfolio", [])
    text = f"🖼 *Портфолио* ({len(portfolio)}/5 фото)\n\nОтправьте фото для добавления."
    buttons = []
    for i, p in enumerate(portfolio):
        label = f"🗑 {p.get('caption', f'Фото {i+1}')[:20]}"
        buttons.append([{"text": label, "callback_data": f"del_photo_{i}"}])
    buttons.append([{"text": "🗑 Удалить всё", "callback_data": "clear_portfolio"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    send_message(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_portfolio_photo(chat_id, file_id):
    master = firestore_get("masters", str(chat_id))
    portfolio = master.get("portfolio", [])
    if len(portfolio) >= 5: return send_message(chat_id, "❌ Максимум 5 фото.", reply_markup=settings_menu())
    portfolio.append({"file_id": file_id, "caption": ""})
    firestore_set("masters", str(chat_id), {"portfolio": portfolio})
    STATES[str(chat_id)] = {"state": "photo_caption", "photo_index": len(portfolio) - 1}
    send_message(chat_id, f"✅ Фото добавлено! ({len(portfolio)}/5)\n\n✏️ Введите подпись:", reply_markup={"keyboard": [["⏩ Пропустить"]], "resize_keyboard": True})

def handle_photo_caption(chat_id, text):
    state = STATES.get(str(chat_id), {})
    if text == "⏩ Пропустить": text = ""
    idx = state.get("photo_index", 0)
    master = firestore_get("masters", str(chat_id))
    portfolio = master.get("portfolio", [])
    if idx < len(portfolio):
        portfolio[idx]["caption"] = text
        firestore_set("masters", str(chat_id), {"portfolio": portfolio})
    STATES.pop(str(chat_id), None)
    send_message(chat_id, "✅ Подпись сохранена!", reply_markup=settings_menu())

def handle_delete_photo(chat_id, idx):
    master = firestore_get("masters", str(chat_id))
    portfolio = master.get("portfolio", [])
    if 0 <= idx < len(portfolio):
        portfolio.pop(idx)
        firestore_set("masters", str(chat_id), {"portfolio": portfolio})
    handle_portfolio_settings(chat_id)

def handle_clear_portfolio(chat_id):
    firestore_set("masters", str(chat_id), {"portfolio": []})
    send_message(chat_id, "🗑 Портфолио очищено.", reply_markup=settings_menu())

def handle_broadcast_start(chat_id):
    STATES[str(chat_id)] = {"state": "broadcast"}
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    phones = set()
    for a in appointments:
        if a.get("client_phone"): phones.add(a.get("client_phone"))
    STATES[str(chat_id)]["broadcast_count"] = len(phones)
    send_message(chat_id, f"📢 *Рассылка*\n\nУ вас {len(phones)} уникальных клиентов.\n\nВведите сообщение:", reply_markup={"keyboard": [["🔙 Отмена"]], "resize_keyboard": True})

def handle_broadcast_preview(chat_id, text):
    count = STATES.get(str(chat_id), {}).get("broadcast_count", 0)
    STATES[str(chat_id)]["broadcast_text"] = text
    send_message(chat_id, f"📢 *Предпросмотр:*\n\n{text}\n\nПолучателей: {count}\n\nОтправить?", reply_markup={"inline_keyboard": [
        [{"text": f"✅ Отправить {count} клиентам", "callback_data": "broadcast_send"}],
        [{"text": "🔙 Отмена", "callback_data": "broadcast_cancel"}]
    ]})

def handle_broadcast_send(chat_id):
    state = STATES.pop(str(chat_id), {})
    text = state.get("broadcast_text", "")
    appointments = firestore_query("appointments", "master_id", "EQUAL", str(chat_id))
    phones = set()
    for a in appointments:
        if a.get("client_phone"): phones.add(a.get("client_phone"))
    sent = 0
    for phone in phones:
        client_appts = firestore_query("appointments", "client_phone", "EQUAL", phone)
        if client_appts and "client_id" in client_appts[0]:
            send_message(int(client_appts[0]["client_id"]), f"📢 *Сообщение от мастера:*\n\n{text}")
            sent += 1
    send_message(chat_id, f"✅ Отправлено {sent} клиентам.", reply_markup=master_menu())

def process_update(update):
    if "message" in update:
        msg = update["message"]
        chat_id, text = msg["chat"]["id"], msg.get("text", "")
        user_name = msg["from"].get("first_name", "Пользователь")
        if "photo" in msg and firestore_get("masters", str(chat_id)):
            handle_portfolio_photo(chat_id, msg["photo"][-1]["file_id"])
            return
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
        if self.path.startswith("/api/calendar"):
            self.send_response(200); self.send_header('Content-type','text/html; charset=utf-8'); self.end_headers()
            chat_id = self.path.split("m=")[-1] if "m=" in self.path else ""
            master = firestore_get("masters", chat_id) if chat_id else None
            if master:
                html = "<html><head><meta charset='utf-8'><style>body{font-family:sans-serif}table{border-collapse:collapse}th,td{border:1px solid #ccc;padding:4px;font-size:12px}th{background:#4CAF50;color:white}td{height:25px}.busy{background:#FFCDD2}.free{background:#C8E6C9}</style></head><body>"
                html += f"<h3>📅 {master.get('name', 'Мастер')}</h3><table><tr><th>Время</th>"
                today = datetime.now()
                for i in range(30): html += f"<th>{(today + timedelta(days=i)).strftime('%d.%m')}</th>"
                html += "</tr>"
                sched = master.get("schedule", {})
                for h in range(7, 22):
                    html += f"<tr><td><b>{h:02d}:00</b></td>"
                    for i in range(30):
                        date = (today + timedelta(days=i)).strftime('%Y-%m-%d')
                        has = any(a.get("date") == date and a.get("time","").startswith(f"{h}:") and a.get("status") != "cancelled" for a in firestore_query("appointments", "master_id", "EQUAL", chat_id))
                        html += "<td class='busy'>📌</td>" if has else "<td class='free'></td>"
                    html += "</tr>"
                html += "</table></body></html>"
                self.wfile.write(html.encode())
            else:
                self.wfile.write(b"Master not found")
        else:
            self.send_response(200); self.send_header('Content-type','application/json'); self.end_headers()
            self.wfile.write(json.dumps({"status":"bot online"}).encode())

app = handler