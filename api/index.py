"""
Мастерилка — бот для записи клиентов
Версия: 3.2.5 (подтверждение записи + улучшения UX)
"""

import os
import json
import traceback
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler
import uuid
import re
import time
import threading
import requests

TOKEN = os.environ.get("TELEGRAM_TOKEN", "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y")
API_KEY = os.environ.get("FIREBASE_API_KEY", "AIzaSyAmP4IW-mcqhXT1L6s4vx5_Z7IZbi1YqI8")
PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "grafikpro-d3500")

TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"
FIRESTORE_URL = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/(default)/documents"

DAYS_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
DAYS_SHORT = ["ПН", "ВТ", "СР", "ЧТ", "ПТ", "СБ", "ВС"]

def now():
    return datetime.now()

def today_str():
    return now().strftime("%Y-%m-%d")

def parse_time(t):
    try:
        parts = t.split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except:
        return 0

def format_time(minutes):
    return f"{minutes // 60:02d}:{minutes % 60:02d}"

def validate_phone(phone):
    clean = re.sub(r'[^0-9+]', '', phone)
    return clean if len(clean) >= 10 else None

class DB:
    @staticmethod
    def get(collection, doc_id):
        try:
            r = requests.get(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}", timeout=8)
            if r.status_code != 200: return None
            return DB._parse(r.json().get("fields", {}))
        except:
            return None
    
    @staticmethod
    def set(collection, doc_id, data):
        try:
            existing = DB.get(collection, doc_id)
            if existing:
                merged = dict(existing)
                for key, val in data.items():
                    merged[key] = val
                data = merged
            fields = DB._serialize(data)
            body = {"fields": fields}
            r = requests.patch(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}", json=body, timeout=8)
            if r.status_code in [200, 201]: return True
            r = requests.post(f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}", json=body, timeout=8)
            return r.status_code in [200, 201]
        except:
            return False
    
    @staticmethod
    def add(collection, data):
        try:
            doc_id = str(uuid.uuid4())[:10]
            r = requests.post(f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}", json={"fields": DB._serialize(data)}, timeout=8)
            return doc_id if r.status_code in [200, 201] else None
        except:
            return None
    
    @staticmethod
    def delete(collection, doc_id):
        try:
            requests.delete(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}", timeout=5)
        except:
            pass
    
    @staticmethod
    def query(collection, field, operator, value):
        try:
            body = {"structuredQuery": {"from": [{"collectionId": collection}], "where": {"fieldFilter": {"field": {"fieldPath": field}, "op": operator, "value": {"stringValue": str(value)}}}}}
            r = requests.post(f"{FIRESTORE_URL}:runQuery?key={API_KEY}", json=body, timeout=8)
            results = []
            if r.status_code == 200:
                for doc in r.json():
                    if "document" in doc:
                        data = DB._parse(doc["document"].get("fields", {}))
                        data["_id"] = doc["document"]["name"].split("/")[-1]
                        results.append(data)
            return results
        except:
            return []
    
    @staticmethod
    def _parse(fields):
        result = {}
        for key, value in fields.items():
            if "stringValue" in value: result[key] = value["stringValue"]
            elif "integerValue" in value: result[key] = int(value["integerValue"])
            elif "doubleValue" in value: result[key] = float(value["doubleValue"])
            elif "booleanValue" in value: result[key] = value["booleanValue"]
            elif "nullValue" in value: result[key] = None
            elif "arrayValue" in value:
                arr = []
                for v in value["arrayValue"].get("values", []):
                    if "stringValue" in v: arr.append(v["stringValue"])
                    elif "integerValue" in v: arr.append(int(v["integerValue"]))
                    elif "mapValue" in v: arr.append(DB._parse(v["mapValue"].get("fields", {})))
                result[key] = arr
            elif "mapValue" in value:
                result[key] = DB._parse(value["mapValue"].get("fields", {}))
        return result
    
    @staticmethod
    def _serialize(data):
        fields = {}
        for key, val in data.items():
            if isinstance(val, str): fields[key] = {"stringValue": val}
            elif isinstance(val, bool): fields[key] = {"booleanValue": val}
            elif isinstance(val, int): fields[key] = {"integerValue": str(val)}
            elif isinstance(val, float): fields[key] = {"doubleValue": val}
            elif val is None: fields[key] = {"nullValue": None}
            elif isinstance(val, list):
                items = []
                for item in val:
                    if isinstance(item, str): items.append({"stringValue": item})
                    elif isinstance(item, int): items.append({"integerValue": str(item)})
                    elif isinstance(item, dict): items.append({"mapValue": {"fields": DB._serialize(item)}})
                fields[key] = {"arrayValue": {"values": items}}
            elif isinstance(val, dict):
                fields[key] = {"mapValue": {"fields": DB._serialize(val)}}
        return fields

class States:
    TTL_MINUTES = 30
    
    @staticmethod
    def get(chat_id):
        data = DB.get("states", str(chat_id))
        if data and "state_data" in data:
            updated_at = data.get("updated_at", "")
            if updated_at:
                try:
                    last_update = datetime.fromisoformat(updated_at)
                    if (now() - last_update).total_seconds() > States.TTL_MINUTES * 60:
                        DB.delete("states", str(chat_id))
                        return {}
                except:
                    pass
            return data["state_data"]
        return {}
    
    @staticmethod
    def set(chat_id, state_data):
        DB.set("states", str(chat_id), {"state_data": state_data, "updated_at": now().isoformat()})
    
    @staticmethod
    def clear(chat_id):
        DB.delete("states", str(chat_id))

class TG:
    @staticmethod
    def send(chat_id, text, reply_markup=None, parse_mode="Markdown"):
        try:
            payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
            if reply_markup: payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
            return requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload, timeout=10).json()
        except:
            return None
    
    @staticmethod
    def answer_callback(callback_id, text=""):
        try:
            requests.post(f"{TELEGRAM_URL}/answerCallbackQuery", json={"callback_query_id": callback_id, "text": text}, timeout=5)
        except:
            pass

class KBD:
    @staticmethod
    def master_main():
        return {"keyboard": [["📊 Сегодня", "📅 Расписание"], ["➕ Новая запись", "👥 Клиенты"], ["🔗 Моя ссылка", "⚙️ Настройки"], ["🔄 Я клиент", "❓ Помощь"]], "resize_keyboard": True}
    
    @staticmethod
    def client_main():
        return {"keyboard": [["📋 Мои записи"], ["🔗 Записаться по ссылке"], ["📤 Поделиться ссылкой"], ["🔍 Найти мастера"], ["🔄 Я мастер", "❓ Помощь"]], "resize_keyboard": True}
    
    @staticmethod
    def settings():
        return {"keyboard": [["💈 Услуги", "⏰ Часы работы"], ["📍 Адрес", "🚷 Чёрный список"], ["📢 Свободные окна", "🖼 Портфолио"], ["🔙 В меню"]], "resize_keyboard": True}
    
    @staticmethod
    def cancel():
        return {"keyboard": [["🔙 Отмена"]], "resize_keyboard": True}
    
    @staticmethod
    def days_schedule(master):
        if not master: return {"inline_keyboard": [[{"text": "Ошибка", "callback_data": "ignore"}]]}
        schedule = master.get("schedule", {})
        buttons = [[{"text": "📋 ПН-ПТ: изменить все будни", "callback_data": "setall_weekdays"}]]
        for i, day_key in enumerate(DAYS_NAMES):
            day_data = schedule.get(day_key)
            label = f"{DAYS_SHORT[i]} {day_data['start']}-{day_data['end']}" if day_data and day_data.get("start") else f"{DAYS_SHORT[i]} выходной"
            buttons.append([{"text": label, "callback_data": f"setday_{day_key}"}])
        buttons.append([{"text": "✅ Готово", "callback_data": "settings_back"}])
        return {"inline_keyboard": buttons}

class Slots:
    @staticmethod
    def get(master_id, date_str, service_duration):
        master = DB.get("masters", master_id)
        if not master: return []
        schedule = master.get("schedule", {})
        try:
            day_key = DAYS_NAMES[datetime.strptime(date_str, "%Y-%m-%d").weekday()]
            day_sched = schedule.get(day_key)
        except:
            return []
        if not day_sched or not day_sched.get("start"): return []
        work_start, work_end = parse_time(day_sched["start"]), parse_time(day_sched["end"])
        appointments = DB.query("appointments", "master_id", "EQUAL", master_id)
        busy = []
        for a in appointments:
            if a.get("date") == date_str and a.get("status") != "cancelled":
                start = parse_time(a.get("time", "00:00"))
                svc = next((s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") == a.get("service")), None)
                busy.append((start, start + (svc.get("duration", 60) if svc else 60)))
        slots, current = [], work_start
        while current + service_duration <= work_end:
            end = current + service_duration
            if all(end <= bs or current >= be for bs, be in busy):
                slots.append(format_time(current))
            current += 30
        return slots

def reminder_worker():
    while True:
        try:
            now_dt = datetime.now()
            for h in [24, 3, 1]:
                rt = (now_dt + timedelta(hours=h)).strftime('%H:%M')
                cd = (now_dt + timedelta(hours=h)).strftime('%Y-%m-%d')
                for a in DB.query("appointments", "date", "EQUAL", cd):
                    if a.get("status") != "confirmed": continue
                    t = a.get("time", "00:00").strip()
                    if ":" not in t and t.isdigit(): t = f"{int(t):02d}:00"
                    if t == rt and not a.get(f"reminded_{h}h"):
                        if h == 1 and "master_id" in a: TG.send(int(a["master_id"]), f"⏰ Через час: {a.get('client_name')} — {a.get('service')}")
                        if "client_id" in a and a["client_id"] != "manual": TG.send(int(a["client_id"]), f"⏰ Напоминание! {a.get('service')} в {a.get('time')}")
                        DB.set("appointments", a["_id"], {f"reminded_{h}h": True})
        except Exception as e:
            print(f"Reminder: {e}")
        time.sleep(60)

threading.Thread(target=reminder_worker, daemon=True).start()

def get_today_summary(chat_id):
    today = today_str()
    apps = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    master = DB.get("masters", str(chat_id))
    svcs = master.get("services", []) if master else []
    today_apps = [a for a in apps if a.get("date") == today and a.get("status") != "cancelled"]
    total = sum(next((s.get("price",0) for s in svcs if isinstance(s, dict) and s.get("name") == a.get("service")), 0) for a in today_apps)
    return f"📊 *Сегодня:* {len(today_apps)} зап, {total}₽" if today_apps else f"📊 *Сегодня:* выходной или нет записей"

def handle_start(chat_id, user_name):
    master = DB.get("masters", str(chat_id))
    if master:
        if not master.get("completed_onboarding"):
            TG.send(chat_id, f"👋 {user_name}!\n\n⚠️ Настройка не завершена.", reply_markup={"inline_keyboard": [[{"text": "🔄 Завершить", "callback_data": "restart_onboarding"}]]})
        else:
            summary = get_today_summary(chat_id)
            TG.send(chat_id, f"👋 {user_name}!\n\n{summary}", reply_markup=KBD.master_main())
    elif DB.get("clients", str(chat_id)):
        TG.send(chat_id, f"👋 {user_name}!", reply_markup=KBD.client_main())
    else:
        TG.send(chat_id, 
            "💈 *Мастерилка — твой личный администратор*\n\n"
            "📅 *Клиенты записываются сами* — ты только принимаешь\n"
            "⏰ *Напоминания за 24, 3 и 1 час* — неявки сократятся в 2 раза\n"
            "⭐ *Рейтинг и портфолио* — клиенты видят твои работы и оценки\n"
            "💰 *Первые 30 записей бесплатно* — попробуй и убедись\n\n"
            "*Кто вы?*",
            reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})

def register_master(chat_id, user_name, username):
    sched = {}
    for d in DAYS_NAMES:
        if d == "sunday": sched[d] = None
        elif d == "saturday": sched[d] = {"start": "10:00", "end": "15:00"}
        else: sched[d] = {"start": "09:00", "end": "18:00"}
    DB.set("masters", str(chat_id), {"name": user_name, "username": username or "", "phone": "", "services": [], "schedule": sched, "breaks": [], "address": "", "portfolio": [], "blacklist": [], "client_notes": {}, "client_tags": {}, "completed_onboarding": False, "onboarding_step": 1, "buffer": 5, "rating": 0, "ratings_count": 0, "created_at": now().isoformat()})
    TG.send(chat_id, f"✅ *{user_name}, добро пожаловать в Мастерилку!*\nСейчас настроим профиль.", reply_markup=KBD.cancel())
    start_onboarding(chat_id)

def start_onboarding(chat_id):
    master = DB.get("masters", str(chat_id))
    if master and master.get("services"):
        svcs = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
        if svcs:
            TG.send(chat_id, f"У вас уже {len(svcs)} услуг. Добавим ещё?", reply_markup={"inline_keyboard": [[{"text": "➕ Добавить", "callback_data": "onboarding_add_more"}], [{"text": "➡️ Дальше", "callback_data": "onboarding_next"}]]})
            return
    DB.set("masters", str(chat_id), {"onboarding_step": 1})
    States.set(chat_id, {"state": "onboarding_services"})
    TG.send(chat_id, "👋 *Шаг 1 из 4: Услуги*\nОтправьте название:", reply_markup={"inline_keyboard": [[{"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}]]})

def onboarding_step_2(chat_id):
    DB.set("masters", str(chat_id), {"onboarding_step": 2})
    States.clear(chat_id)
    TG.send(chat_id, "⏰ *Шаг 2 из 4: Часы*", reply_markup=KBD.days_schedule(DB.get("masters", str(chat_id))))

def onboarding_step_3(chat_id):
    DB.set("masters", str(chat_id), {"onboarding_step": 3})
    States.set(chat_id, {"state": "onboarding_address"})
    TG.send(chat_id, "📍 *Шаг 3 из 4: Адрес*", reply_markup={"inline_keyboard": [[{"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}], [{"text": "🔙 Назад", "callback_data": "back_to_step2"}]]})

def onboarding_step_4(chat_id):
    DB.set("masters", str(chat_id), {"onboarding_step": 4})
    States.set(chat_id, {"state": "onboarding_portfolio"})
    TG.send(chat_id, "🖼 *Шаг 4 из 4: Портфолио*", reply_markup={"inline_keyboard": [[{"text": "⏩ Завершить", "callback_data": "onboarding_finish"}], [{"text": "🔙 Назад", "callback_data": "back_to_step3"}]]})

def finish_onboarding(chat_id):
    if not DB.get("masters", str(chat_id)): return TG.send(chat_id, "❌ Ошибка. /start")
    DB.set("masters", str(chat_id), {"completed_onboarding": True, "onboarding_step": 0})
    States.clear(chat_id)
    TG.send(chat_id, "🎉 *Готово!*", reply_markup=KBD.master_main())
    show_master_link(chat_id)

def show_master_link(chat_id):
    if not DB.get("masters", str(chat_id)): return TG.send(chat_id, "❌ Зарегистрируйтесь.")
    links = DB.query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: DB.set("links", link_id, {"master_id": str(chat_id)})
    TG.send(chat_id, f"🔗 *Ваша ссылка:*\n`https://t.me/MasterilkaBot?start=master_{link_id}`")

def start_add_service(chat_id):
    States.set(chat_id, {"state": "adding_service_name"})
    TG.send(chat_id, "✏️ Название (до 100 символов):", reply_markup=KBD.cancel())

def handle_service_name(chat_id, name):
    name = name.strip()
    if len(name) < 2 or len(name) > 100: return TG.send(chat_id, "❌ От 2 до 100 символов.")
    States.set(chat_id, {"state": "adding_service_price", "svc_name": name})
    TG.send(chat_id, f"💰 Цена:")

def handle_service_price(chat_id, text):
    try:
        p = int(text.strip())
        if p <= 0: raise ValueError
    except:
        return TG.send(chat_id, "❌ Положительное число.")
    s = States.get(chat_id)
    States.set(chat_id, {"state": "adding_service_duration", "svc_name": s.get("svc_name"), "svc_price": p})
    TG.send(chat_id, f"⏱ Длительность (мин):")

def handle_service_duration(chat_id, text):
    try:
        d = int(text.strip())
        if d <= 0 or d > 480: raise ValueError
    except:
        return TG.send(chat_id, "❌ От 1 до 480.")
    s = States.get(chat_id)
    if not s: return TG.send(chat_id, "❌ Сессия истекла.", reply_markup=KBD.settings())
    return save_service(chat_id, s["svc_name"], s["svc_price"], d)

def save_service(chat_id, name, price, duration):
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Зарегистрируйтесь: /start")
    svcs = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    svcs.append({"name": name, "price": price, "duration": duration, "disabled": False})
    DB.set("masters", str(chat_id), {"services": svcs})
    States.clear(chat_id)
    TG.send(chat_id, f"✅ *{name}* — {price}₽, {duration}мин", reply_markup=KBD.settings())
    return True

def delete_service(chat_id, name):
    master = DB.get("masters", str(chat_id))
    if master:
        DB.set("masters", str(chat_id), {"services": [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") != name]})
    handle_services_settings(chat_id)

def handle_services_settings(chat_id):
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ /start")
    svcs = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    text = "💈 *Услуги:*\n" + "\n".join([f"• {s['name']} — {s.get('price',0)}₽ ({s.get('duration',60)}мин)" for s in svcs]) if svcs else "💈 Нет услуг"
    buttons = [[{"text": f"🗑 {s['name']}", "callback_data": f"delservice_{s['name']}"}] for s in svcs]
    buttons.append([{"text": "➕ Добавить", "callback_data": "addservice"}, {"text": "🔙 Назад", "callback_data": "settings_back"}])
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_set_all_weekdays(chat_id):
    States.set(chat_id, {"state": "setting_all_weekdays"})
    TG.send(chat_id, "📋 *Будни (ПН-ПТ)*\nВведите: `09:00-18:00`", reply_markup=KBD.cancel())

def handle_set_all_weekdays_value(chat_id, text):
    try:
        st, en = text.strip().split("-")
        st, en = st.strip(), en.strip()
    except:
        return TG.send(chat_id, "❌ Формат: 09:00-18:00")
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    sched = master.get("schedule", {})
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        sched[day] = {"start": st, "end": en}
    DB.set("masters", str(chat_id), {"schedule": sched})
    States.clear(chat_id)
    TG.send(chat_id, f"✅ ПН-ПТ: {st}-{en}", reply_markup=KBD.days_schedule(DB.get("masters", str(chat_id))))

def handle_set_day_schedule(chat_id, day_key):
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    cur = master.get("schedule", {}).get(day_key, {})
    txt = "выходной" if not cur or not cur.get("start") else f"{cur['start']} – {cur['end']}"
    dn = {"monday":"Пн","tuesday":"Вт","wednesday":"Ср","thursday":"Чт","friday":"Пт","saturday":"Сб","sunday":"Вс"}
    States.set(chat_id, {"state": "setting_day", "day_key": day_key})
    TG.send(chat_id, f"⏰ *{dn.get(day_key,day_key)}*\nСейчас: {txt}\nВыберите или введите `09:00-18:00` / `выходной`", reply_markup={"inline_keyboard": [[{"text": "09:00-18:00", "callback_data": f"setdayvalue_{day_key}_09:00-18:00"}], [{"text": "10:00-20:00", "callback_data": f"setdayvalue_{day_key}_10:00-20:00"}], [{"text": "🚫 Выходной", "callback_data": f"setdayvalue_{day_key}_выходной"}], [{"text": "🔙 Назад", "callback_data": "back_to_days"}]]})

def handle_set_day_value(chat_id, day_key, value):
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    sched = master.get("schedule", {})
    if value == "выходной": sched[day_key] = None
    else:
        try:
            st, en = value.split("-")
            sched[day_key] = {"start": st.strip(), "end": en.strip()}
        except:
            return TG.send(chat_id, "❌ Формат: 09:00-18:00")
    DB.set("masters", str(chat_id), {"schedule": sched})
    States.clear(chat_id)
    TG.send(chat_id, "✅ Обновлён!", reply_markup=KBD.days_schedule(DB.get("masters", str(chat_id))))

def start_set_address(chat_id):
    States.set(chat_id, {"state": "setting_address"})
    TG.send(chat_id, "📍 Адрес:", reply_markup=KBD.cancel())

def handle_address_set(chat_id, addr):
    DB.set("masters", str(chat_id), {"address": addr.strip()})
    States.clear(chat_id)
    TG.send(chat_id, f"✅ {addr.strip()}", reply_markup=KBD.settings())

def show_blacklist(chat_id):
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    bl = master.get("blacklist", [])
    text = "🚷 *ЧС:*\n" + "\n".join([f"• {b.get('phone','')}" for b in bl]) if bl else "🚷 Пуст"
    buttons = [[{"text": "➕ Добавить", "callback_data": "add_blacklist"}]]
    for b in bl: buttons.append([{"text": f"🗑 {b.get('phone','')}", "callback_data": f"remove_blacklist_{b.get('phone','')}"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def start_add_blacklist(chat_id):
    States.set(chat_id, {"state": "adding_blacklist"})
    TG.send(chat_id, "🚷 Номер:", reply_markup=KBD.cancel())

def handle_add_blacklist(chat_id, phone):
    phone = validate_phone(phone)
    if not phone: return TG.send(chat_id, "❌ Неверный формат.")
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    bl = master.get("blacklist", [])
    if any(b.get("phone") == phone for b in bl): TG.send(chat_id, "❌ Уже в списке.")
    else:
        bl.append({"phone": phone})
        DB.set("masters", str(chat_id), {"blacklist": bl})
        TG.send(chat_id, f"✅ {phone}")
    States.clear(chat_id)
    show_blacklist(chat_id)

def handle_remove_blacklist(chat_id, phone):
    master = DB.get("masters", str(chat_id))
    if master:
        DB.set("masters", str(chat_id), {"blacklist": [b for b in master.get("blacklist", []) if b.get("phone") != phone]})
    show_blacklist(chat_id)

def handle_client_booking_start(chat_id, link_id):
    if not DB.get("clients", str(chat_id)): DB.set("clients", str(chat_id), {"created_at": now().isoformat()})
    link = DB.get("links", link_id)
    if not link: return TG.send(chat_id, "❌ Ссылка недействительна.")
    master = DB.get("masters", link["master_id"])
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    svcs = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") and not s.get("disabled")]
    if not svcs: return TG.send(chat_id, "😔 *Мастер пока не добавил услуги.*\n\nПожалуйста, свяжитесь с ним напрямую или попробуйте позже.")
    States.set(chat_id, {"state": "client_booking", "master_id": link["master_id"], "master_name": master.get("name","Мастер"), "master_addr": master.get("address",""), "services": svcs})
    text = f"👤 *{master.get('name','Мастер')}*\n"
    if master.get("address"): text += f"📍 {master['address']}\n"
    text += "\nВыберите услугу:"
    buttons = [[{"text": f"{s['name']} — {s['price']}₽ ({s['duration']}мин)", "callback_data": f"bkservice_{s['name']}"}] for s in svcs]
    buttons.append([{"text": "🔙 Отмена", "callback_data": "booking_cancel"}])
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_booking_service(chat_id, svc_name):
    s = States.get(chat_id)
    s["service"], s["state"] = svc_name, "booking_date"
    States.set(chat_id, s)
    buttons = [[{"text": (now()+timedelta(days=i+1)).strftime('%d.%m')+" "+DAYS_SHORT[(now()+timedelta(days=i+1)).weekday()], "callback_data": f"bkdate_{(now()+timedelta(days=i+1)).strftime('%Y-%m-%d')}"}] for i in range(14)]
    buttons.append([{"text": "🔙 К услугам", "callback_data": "booking_back_to_svc"}])
    TG.send(chat_id, f"💈 *{svc_name}*\nДата:", reply_markup={"inline_keyboard": buttons})

def handle_booking_date(chat_id, date_str):
    s = States.get(chat_id)
    mid = s.get("master_id")
    if not mid: return TG.send(chat_id, "❌ Сессия истекла. Попробуйте снова по ссылке мастера.")
    master = DB.get("masters", mid)
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    svc = next((x for x in master.get("services", []) if isinstance(x, dict) and x.get("name") == s.get("service")), None)
    free = Slots.get(mid, date_str, svc.get("duration", 60) if svc else 60)
    if not free: return TG.send(chat_id, f"📭 Нет слотов на {date_str}", reply_markup={"inline_keyboard": [[{"text": "🔙 К услугам", "callback_data": "booking_back_to_svc"}]]})
    s["date"], s["state"] = date_str, "booking_time"
    States.set(chat_id, s)
    buttons = [[{"text": f"🟢 {t}", "callback_data": f"bktime_{t}"}] for t in free]
    buttons.append([{"text": "🔙 К услугам", "callback_data": "booking_back_to_svc"}])
    TG.send(chat_id, f"📅 *{date_str}*\nВремя:", reply_markup={"inline_keyboard": buttons})

def handle_booking_time(chat_id, time_str):
    s = States.get(chat_id)
    s["time"], s["state"] = time_str, "booking_confirm"
    States.set(chat_id, s)
    master = DB.get("masters", s.get("master_id"))
    TG.send(chat_id,
        f"📋 *Проверьте данные:*\n\n"
        f"👤 Мастер: *{master.get('name','')}*\n"
        f"💈 Услуга: *{s.get('service','')}*\n"
        f"📅 Дата: *{s.get('date','')}*\n"
        f"⏰ Время: *{time_str}*\n\n"
        f"Всё верно?",
        reply_markup={"inline_keyboard": [
            [{"text": "✅ Подтвердить", "callback_data": f"bkconfirm_{time_str}"}],
            [{"text": "🔄 Выбрать другое время", "callback_data": f"bkservice_{s.get('service','')}"}],
            [{"text": "🔙 Отмена", "callback_data": "booking_cancel"}]
        ]})

def handle_booking_confirm(chat_id, time_str):
    s = States.get(chat_id)
    if not s: return TG.send(chat_id, "❌ Сессия истекла.")
    s["time"] = time_str
    s["state"] = "booking_name"
    States.set(chat_id, s)
    TG.send(chat_id, "📝 Ваше имя:", reply_markup=KBD.cancel())

def handle_booking_name(chat_id, name):
    name = name.strip()
    if len(name) < 2: return TG.send(chat_id, "❌ Минимум 2 символа.")
    s = States.get(chat_id)
    s["client_name"], s["state"] = name, "booking_phone"
    States.set(chat_id, s)
    TG.send(chat_id, "📞 Телефон:", reply_markup=KBD.cancel())

def handle_booking_phone(chat_id, phone):
    phone = validate_phone(phone)
    if not phone: return TG.send(chat_id, "❌ Неверный формат.")
    s = States.get(chat_id)
    if not s: return TG.send(chat_id, "❌ Сессия истекла. Попробуйте снова по ссылке мастера.")
    mid = s.get("master_id")
    if not mid: return TG.send(chat_id, "❌ Мастер не найден.")
    master = DB.get("masters", mid)
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    if any(b.get("phone") == phone for b in master.get("blacklist", [])): return TG.send(chat_id, "❌ Запись невозможна.", reply_markup=KBD.client_main())
    doc_id = DB.add("appointments", {"master_id": mid, "client_id": str(chat_id), "client_name": s["client_name"], "client_phone": phone, "service": s["service"], "date": s["date"], "time": s["time"], "status": "confirmed", "reminded_24h": False, "reminded_3h": False, "reminded_1h": False, "created_at": now().isoformat()})
    if not doc_id: return TG.send(chat_id, "❌ Ошибка.", reply_markup=KBD.client_main())
    States.clear(chat_id)
    cf = f"✅ *Запись подтверждена!*\n\n👤 {master.get('name','')}\n💈 {s['service']}\n📅 {s['date']} в {s['time']}"
    if s.get("master_addr"): cf += f"\n📍 {s['master_addr']}"
    TG.send(chat_id, cf + f"\n\n📞 {phone}", reply_markup=KBD.client_main())
    TG.send(int(mid), f"🔔 *Новая запись!*\n👤 {s['client_name']}\n📞 {phone}\n💈 {s['service']}\n📅 {s['date']} в {s['time']}", reply_markup={"inline_keyboard": [[{"text": "👤 Клиент", "callback_data": f"client_card_{phone}"}], [{"text": "📅 Расписание", "callback_data": "schedule_filter_today"}]]})

def handle_client_booking_by_link(chat_id):
    States.set(chat_id, {"state": "entering_master_link"})
    TG.send(chat_id, "🔗 *Вставьте ссылку мастера:*\n\nНапример: `https://t.me/MasterilkaBot?start=master_abc123`", reply_markup=KBD.cancel())

def handle_enter_master_link(chat_id, text):
    if "master_" in text:
        link_id = text.split("master_")[1].split()[0].split("?")[0]
        States.clear(chat_id)
        handle_client_booking_start(chat_id, link_id)
    else:
        TG.send(chat_id, "❌ Неверная ссылка. Попробуйте ещё раз или нажмите Отмена.", reply_markup=KBD.cancel())

def start_manual_booking(chat_id):
    States.set(chat_id, {"state": "manual_name"})
    TG.send(chat_id, "📝 *Новая запись*\nИмя:", reply_markup=KBD.cancel())

def handle_manual_name(chat_id, name):
    if len(name.strip()) < 2: return TG.send(chat_id, "❌ Минимум 2 символа.")
    States.set(chat_id, {"state": "manual_phone", "client_name": name.strip()})
    TG.send(chat_id, "📞 Телефон:")

def handle_manual_phone(chat_id, phone):
    phone = validate_phone(phone)
    if not phone: return TG.send(chat_id, "❌ Неверный формат.")
    s = States.get(chat_id)
    if not s: return TG.send(chat_id, "❌ Сессия истекла.", reply_markup=KBD.master_main())
    s["client_phone"], s["state"] = phone, "manual_service"
    States.set(chat_id, s)
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    svcs = [x for x in master.get("services", []) if isinstance(x, dict) and x.get("name")]
    if not svcs: return TG.send(chat_id, "❌ Нет услуг.")
    TG.send(chat_id, "💈 Услуга:", reply_markup={"inline_keyboard": [[{"text": f"{x['name']} ({x['price']}₽)", "callback_data": f"manservice_{x['name']}"}] for x in svcs]})

def handle_manual_service(chat_id, svc_name):
    s = States.get(chat_id)
    s["service"], s["state"] = svc_name, "manual_date"
    States.set(chat_id, s)
    buttons = [[{"text": "Сегодня" if i==0 else (now()+timedelta(days=i)).strftime('%d.%m'), "callback_data": f"mandate_{(now()+timedelta(days=i)).strftime('%Y-%m-%d')}"}] for i in range(14)]
    TG.send(chat_id, "📅 Дата:", reply_markup={"inline_keyboard": buttons})

def handle_manual_date(chat_id, date_str):
    s = States.get(chat_id)
    master = DB.get("masters", str(chat_id))
    if not master: return TG.send(chat_id, "❌ Мастер не найден.")
    svc = next((x for x in master.get("services", []) if isinstance(x, dict) and x.get("name") == s.get("service")), None)
    free = Slots.get(str(chat_id), date_str, svc.get("duration", 60) if svc else 60)
    if not free: return TG.send(chat_id, "📭 Нет слотов.")
    s["date"], s["state"] = date_str, "manual_time"
    States.set(chat_id, s)
    TG.send(chat_id, f"⏰ Время на {date_str}:", reply_markup={"inline_keyboard": [[{"text": f"🟢 {t}", "callback_data": f"mantime_{t}"}] for t in free]})

def handle_manual_time(chat_id, time_str):
    s = States.get(chat_id)
    States.clear(chat_id)
    if not s: return TG.send(chat_id, "❌ Сессия истекла.", reply_markup=KBD.master_main())
    DB.add("appointments", {"master_id": str(chat_id), "client_id": "manual", "client_name": s.get("client_name",""), "client_phone": s.get("client_phone",""), "service": s.get("service",""), "date": s.get("date",""), "time": time_str, "status": "confirmed", "reminded_24h": False, "reminded_3h": False, "reminded_1h": False, "created_at": now().isoformat()})
    TG.send(chat_id, f"✅ {s['client_name']}\n{s['service']}\n{s['date']} в {time_str}", reply_markup=KBD.master_main())

def show_schedule(chat_id, mode="all"):
    apps = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    if not apps: return TG.send(chat_id, "📭 Нет записей.")
    today, tomorrow, week_end = today_str(), (now()+timedelta(days=1)).strftime("%Y-%m-%d"), (now()+timedelta(days=7)).strftime("%Y-%m-%d")
    if mode == "today": apps = [a for a in apps if a.get("date") == today]
    elif mode == "tomorrow": apps = [a for a in apps if a.get("date") == tomorrow]
    elif mode == "week": apps = [a for a in apps if today <= a.get("date","") <= week_end]
    apps = [a for a in apps if a.get("status") != "cancelled"]
    apps.sort(key=lambda a: (a.get("date",""), a.get("time","")))
    if not apps: return TG.send(chat_id, "📭 Нет записей.")
    master = DB.get("masters", str(chat_id))
    svcs = master.get("services", []) if master else []
    text = "📅 *Расписание:*\n"
    buttons = []
    for a in apps[:15]:
        icon = {"confirmed":"🟡","completed":"✅","no_show":"❌"}.get(a.get("status"),"")
        svc = next((s for s in svcs if isinstance(s, dict) and s.get("name") == a.get("service")), None)
        dur = svc.get("duration", 60) if svc else 60
        end_time = format_time(parse_time(a.get("time","00:00")) + dur)
        text += f"\n{icon} *{a.get('date')}* {a.get('time')} – {end_time}\n  {a.get('service')} — {a.get('client_name','?')} | {a.get('client_phone','?')}"
        if a.get("status") == "confirmed":
            buttons.append([{"text": f"✅ Вып: {a.get('date')} {a.get('time')}", "callback_data": f"complete_{a['_id']}"}])
            buttons.append([{"text": f"❌ Неявка: {a.get('date')} {a.get('time')}", "callback_data": f"noshow_{a['_id']}"}])
            buttons.append([{"text": f"🔄 Перенести: {a.get('date')} {a.get('time')}", "callback_data": f"reschedule_{a['_id']}"}])
            buttons.append([{"text": f"🗑 Удалить: {a.get('date')} {a.get('time')}", "callback_data": f"delete_{a['_id']}"}])
    filter_buttons = [[{"text": f, "callback_data": f"schedule_filter_{f}"} for f in ["all","today","tomorrow","week"]]]
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons + filter_buttons} if buttons else {"inline_keyboard": filter_buttons})

def show_dashboard(chat_id):
    today, apps = today_str(), DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    master = DB.get("masters", str(chat_id))
    svcs = master.get("services", []) if master else []
    ta = [a for a in apps if a.get("date") == today and a.get("status") != "cancelled"]
    mc = [a for a in apps if a.get("status") == "completed" and a.get("date","") >= (now()-timedelta(days=30)).strftime("%Y-%m-%d")]
    total_today = sum(next((s.get("price",0) for s in svcs if isinstance(s, dict) and s.get("name") == a.get("service")), 0) for a in ta)
    total_month = sum(next((s.get("price",0) for s in svcs if isinstance(s, dict) and s.get("name") == a.get("service")), 0) for a in mc)
    TG.send(chat_id, f"📊 *Дашборд*\n\n📅 Сегодня: {len(ta)} зап, {total_today}₽\n📆 За 30 дней: {len(mc)} вып, {total_month}₽", reply_markup=KBD.master_main())

def show_clients(chat_id):
    apps = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    if not apps: return TG.send(chat_id, "👥 Нет клиентов.")
    clients = {}
    for a in apps:
        p = a.get("client_phone","нет")
        if p not in clients: clients[p] = {"name": a.get("client_name","?"), "phone": p, "count": 0, "last": ""}
        clients[p]["count"] += 1
        if a.get("date","") > clients[p]["last"]: clients[p]["last"] = a.get("date","")
    text = "👥 *Клиенты:*\n"
    buttons = []
    for p, d in list(clients.items())[:15]:
        text += f"\n• *{d['name']}* — {p}\n  {d['count']} виз, посл: {d['last']}"
        buttons.append([{"text": f"👤 {d['name']}", "callback_data": f"client_card_{p}"}])
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def show_client_card(chat_id, phone):
    apps = DB.query("appointments", "client_phone", "EQUAL", phone)
    master = DB.get("masters", str(chat_id))
    note = master.get("client_notes", {}).get(phone, "") if master else ""
    tag = master.get("client_tags", {}).get(phone, "") if master else ""
    total = len([a for a in apps if a.get("status") == "completed"])
    text = f"👤 *Клиент: {phone}*\n"
    if tag: text += f"🏷 {tag}\n"
    if note: text += f"📝 {note}\n"
    text += f"\n📊 Визитов: {total}\n\n📋 *История:*\n"
    for a in sorted(apps, key=lambda x: x.get("date",""), reverse=True)[:10]:
        icon = {"confirmed":"🟡","completed":"✅","no_show":"❌","cancelled":"🗑"}.get(a.get("status"),"")
        text += f"{icon} {a.get('date')} — {a.get('service')}\n"
    buttons = [[{"text": "📝 Заметка", "callback_data": f"add_note_{phone}"}], [{"text": "🏷 Теги", "callback_data": f"edit_tags_{phone}"}], [{"text": "💬 Написать", "url": f"tg://resolve?phone={phone}"}], [{"text": "📞 Позвонить", "url": f"tel:{phone}"}]]
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def show_free_slots(chat_id):
    TG.send(chat_id, "📅 *Окна*\nДень:", reply_markup={"inline_keyboard": [[{"text": "Сегодня" if i==0 else "Завтра" if i==1 else (now()+timedelta(days=i)).strftime('%d.%m'), "callback_data": f"freeslots_{(now()+timedelta(days=i)).strftime('%Y-%m-%d')}"}] for i in range(7)]})

def show_free_slots_day(chat_id, date_str):
    master = DB.get("masters", str(chat_id))
    if not master: return
    svcs = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    if not svcs: return TG.send(chat_id, "❌ Нет услуг.")
    dur = svcs[0].get("duration", 60) if isinstance(svcs[0], dict) else 60
    free = Slots.get(str(chat_id), date_str, dur)
    TG.send(chat_id, f"🟢 *{date_str}:*\n" + "\n".join([f"• {t}" for t in free]) if free else f"📭 {date_str} — занято")

def handle_client_appointments(chat_id):
    apps = DB.query("appointments", "client_id", "EQUAL", str(chat_id))
    active = [a for a in apps if a.get("status") != "cancelled"]
    if not active: return TG.send(chat_id, "📋 Нет записей.")
    active.sort(key=lambda a: (a.get("date",""), a.get("time","")))
    master_cache = {}
    text, buttons = "📋 *Мои записи:*\n", []
    for a in active:
        mid = a.get("master_id","")
        if mid not in master_cache:
            master_cache[mid] = DB.get("masters", mid)
        master = master_cache[mid]
        mn = master.get("name","Мастер") if master else "Мастер"
        addr = master.get("address","") if master else ""
        text += f"\n• {a.get('date')} в {a.get('time')}\n  {a.get('service')} у {mn}"
        if addr: text += f"\n  📍 {addr}"
        if a.get("status") == "confirmed":
            buttons.append([{"text": f"🔄 Перенести: {a.get('date')} {a.get('time')}", "callback_data": f"cl_reschedule_{a['_id']}"}])
            buttons.append([{"text": f"❌ Отменить: {a.get('date')} {a.get('time')}", "callback_data": f"cancel_{a['_id']}"}])
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_client_reschedule_start(chat_id, appt_id):
    a = DB.get("appointments", appt_id)
    if not a or a.get("client_id") != str(chat_id): return TG.send(chat_id, "❌ Ошибка.")
    States.set(chat_id, {"state": "client_reschedule_date", "appt_id": appt_id})
    buttons = [[{"text": (now()+timedelta(days=i+1)).strftime('%d.%m')+" "+DAYS_SHORT[(now()+timedelta(days=i+1)).weekday()], "callback_data": f"cl_res_date_{appt_id}_{(now()+timedelta(days=i+1)).strftime('%Y-%m-%d')}"}] for i in range(7)]
    TG.send(chat_id, "🔄 *Перенос*\nНовая дата:", reply_markup={"inline_keyboard": buttons})

def handle_client_reschedule_date(chat_id, appt_id, date):
    States.set(chat_id, {"state": "client_reschedule_time", "appt_id": appt_id, "new_date": date})
    a = DB.get("appointments", appt_id)
    master = DB.get("masters", a["master_id"])
    free = Slots.get(a["master_id"], date, next((s.get("duration",60) for s in master.get("services",[]) if isinstance(s, dict) and s.get("name") == a.get("service")), 60))
    if not free: return TG.send(chat_id, "📭 Нет слотов.")
    TG.send(chat_id, f"⏰ Время на {date}:", reply_markup={"inline_keyboard": [[{"text": f"🟢 {t}", "callback_data": f"cl_res_time_{appt_id}_{date}_{t}"}] for t in free]})

def handle_client_reschedule_time(chat_id, appt_id, date, time):
    DB.set("appointments", appt_id, {"date": date, "time": time, "reminded_24h": False, "reminded_3h": False, "reminded_1h": False})
    a = DB.get("appointments", appt_id)
    TG.send(int(a["master_id"]), f"🔄 *Перенос!*\n{a.get('client_name')}\n{a.get('service')}\nНовое: {date} в {time}")
    States.clear(chat_id)
    TG.send(chat_id, f"✅ Перенесено на {date} {time}", reply_markup=KBD.client_main())

def handle_cancel_appointment(chat_id, appt_id):
    a = DB.get("appointments", appt_id)
    if not a or a.get("client_id") != str(chat_id): return TG.send(chat_id, "❌ Ошибка.")
    if a.get("master_id"): TG.send(int(a["master_id"]), f"❌ *Отмена!*\n{a.get('client_name')} отменил {a.get('service')} {a.get('date')} в {a.get('time')}")
    DB.set("appointments", appt_id, {"status": "cancelled"})
    TG.send(chat_id, "✅ Отменено.", reply_markup=KBD.client_main())

def handle_master_delete_appointment(chat_id, appt_id):
    a = DB.get("appointments", appt_id)
    if not a: return TG.send(chat_id, "❌ Запись не найдена.")
    if a.get("client_id") and a.get("client_id") != "manual": TG.send(int(a["client_id"]), f"❌ *Запись отменена мастером*\n{a.get('service')}\n{a.get('date')} в {a.get('time')}")
    DB.set("appointments", appt_id, {"status": "cancelled"})
    TG.send(chat_id, "🗑 Запись удалена.", reply_markup=KBD.master_main())

def handle_complete_appointment(chat_id, appt_id):
    DB.set("appointments", appt_id, {"status": "completed"})
    a = DB.get("appointments", appt_id)
    if a and a.get("client_id") and a.get("client_id") != "manual":
        TG.send(int(a["client_id"]), f"⭐ *Оцените!*\n{a.get('service')}", reply_markup={"inline_keyboard": [[{"text": f"{'⭐'*i}", "callback_data": f"rate_{a['master_id']}_{i}"}] for i in range(1,6)]})
    TG.send(chat_id, "✅ Выполнено!", reply_markup=KBD.master_main())

def handle_noshow_appointment(chat_id, appt_id):
    DB.set("appointments", appt_id, {"status": "no_show"})
    TG.send(chat_id, "❌ Неявка.", reply_markup=KBD.master_main())

def handle_reschedule_start(chat_id, appt_id):
    a = DB.get("appointments", appt_id)
    if not a: return TG.send(chat_id, "Запись не найдена.")
    States.set(chat_id, {"state": "reschedule_date", "appt_id": appt_id})
    buttons = [[{"text": (now()+timedelta(days=i)).strftime('%d.%m'), "callback_data": f"res_date_{appt_id}_{(now()+timedelta(days=i)).strftime('%Y-%m-%d')}"}] for i in range(14)]
    TG.send(chat_id, "📅 Новая дата:", reply_markup={"inline_keyboard": buttons})

def handle_reschedule_date(chat_id, appt_id, date):
    States.set(chat_id, {"state": "reschedule_time", "appt_id": appt_id, "new_date": date})
    a = DB.get("appointments", appt_id)
    master = DB.get("masters", a["master_id"])
    free = Slots.get(a["master_id"], date, next((s.get("duration",60) for s in master.get("services",[]) if isinstance(s, dict) and s.get("name") == a.get("service")), 60))
    if not free: return TG.send(chat_id, "📭 Нет слотов.")
    TG.send(chat_id, f"⏰ Время на {date}:", reply_markup={"inline_keyboard": [[{"text": f"🟢 {t}", "callback_data": f"res_time_{appt_id}_{date}_{t}"}] for t in free]})

def handle_reschedule_time(chat_id, appt_id, date, time):
    DB.set("appointments", appt_id, {"date": date, "time": time, "reminded_24h": False, "reminded_3h": False, "reminded_1h": False})
    a = DB.get("appointments", appt_id)
    if a.get("client_id") and a.get("client_id") != "manual": TG.send(int(a["client_id"]), f"🔄 *Перенесено!*\n{a.get('service')}\nНовое: {date} в {time}")
    States.clear(chat_id)
    TG.send(chat_id, f"✅ Перенесено на {date} {time}", reply_markup=KBD.master_main())

def handle_find_master(chat_id, phone):
    phone = validate_phone(phone)
    if not phone: return TG.send(chat_id, "❌ Неверный формат.")
    masters = DB.query("masters", "phone", "EQUAL", phone)
    if not masters: States.clear(chat_id); return TG.send(chat_id, "❌ Не найден.", reply_markup=KBD.client_main())
    m = masters[0]
    svcs = [s for s in m.get("services", []) if isinstance(s, dict) and s.get("name") and not s.get("disabled")]
    addr = m.get("address","Не указан")
    links = DB.query("links", "master_id", "EQUAL", m.get("_id",""))
    lid = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: DB.set("links", lid, {"master_id": m.get("_id","")})
    States.set(chat_id, {"state": "client_booking", "master_id": m.get("_id",""), "master_name": m.get("name",""), "master_addr": m.get("address",""), "services": svcs})
    buttons = [[{"text": "📝 Записаться", "callback_data": f"bkservice_{svcs[0]['name']}"}]] if svcs else []
    TG.send(chat_id, f"👤 *{m.get('name')}*\n📍 {addr}\n\n💈 *Услуги:*\n" + "\n".join([f"• {s['name']} — {s['price']}₽" for s in svcs]), reply_markup={"inline_keyboard": buttons} if buttons else None)

def handle_share_link(chat_id):
    apps = DB.query("appointments", "client_id", "EQUAL", str(chat_id))
    if not apps: return TG.send(chat_id, "📤 У вас пока нет записей.")
    last_app = max(apps, key=lambda a: a.get("date",""))
    master_id = last_app.get("master_id","")
    if not master_id: return TG.send(chat_id, "❌ Мастер не найден.")
    links = DB.query("links", "master_id", "EQUAL", master_id)
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    if not links: DB.set("links", link_id, {"master_id": master_id})
    link = f"https://t.me/MasterilkaBot?start=master_{link_id}"
    TG.send(chat_id, f"📤 *Поделитесь ссылкой на мастера:*\n\n`{link}`\n\nОтправьте другу!")

def handle_text(chat_id, user_name, username, text):
    sd = States.get(chat_id)
    state = sd.get("state", "")
    master, client = DB.get("masters", str(chat_id)), DB.get("clients", str(chat_id))
    
    if text == "🔙 Отмена":
        States.clear(chat_id)
        return TG.send(chat_id, "❌ Отменено", reply_markup=KBD.master_main() if master else KBD.client_main())
    
    if state == "adding_service_name": return handle_service_name(chat_id, text)
    if state == "adding_service_price": return handle_service_price(chat_id, text)
    if state == "adding_service_duration": return handle_service_duration(chat_id, text)
    if state == "setting_address": return handle_address_set(chat_id, text)
    if state == "adding_blacklist": return handle_add_blacklist(chat_id, text)
    if state == "setting_day": return handle_set_day_value(chat_id, sd.get("day_key",""), text)
    if state == "setting_all_weekdays": return handle_set_all_weekdays_value(chat_id, text)
    if state == "onboarding_address": DB.set("masters", str(chat_id), {"address": text.strip()}); States.clear(chat_id); TG.send(chat_id, "✅ Адрес сохранён!"); return onboarding_step_4(chat_id)
    if state == "booking_name": return handle_booking_name(chat_id, text)
    if state == "booking_phone": return handle_booking_phone(chat_id, text)
    if state == "entering_master_link": return handle_enter_master_link(chat_id, text)
    if state == "adding_note":
        phone = sd.get("note_phone","")
        master_data = DB.get("masters", str(chat_id))
        notes = master_data.get("client_notes", {}) if master_data else {}
        notes[phone] = text.strip()
        DB.set("masters", str(chat_id), {"client_notes": notes})
        States.clear(chat_id)
        return TG.send(chat_id, "✅ Заметка сохранена!", reply_markup=KBD.master_main())
    if state == "onboarding_services":
        if len(text.strip()) < 2: return TG.send(chat_id, "❌ Короткое название")
        States.set(chat_id, {"state": "onboarding_service_price", "svc_name": text.strip()})
        return TG.send(chat_id, "💰 Цена:")
    if state == "onboarding_service_price":
        try: p = int(text.strip())
        except: return TG.send(chat_id, "❌ Число")
        States.set(chat_id, {"state": "onboarding_service_duration", "svc_name": sd.get("svc_name",""), "svc_price": p})
        return TG.send(chat_id, "⏱ Длительность (мин):")
    if state == "onboarding_service_duration":
        try: d = int(text.strip())
        except: return TG.send(chat_id, "❌ Число")
        name, price = sd.get("svc_name",""), sd.get("svc_price",0)
        save_service(chat_id, name, price, d)
        return TG.send(chat_id, f"✅ *{name}* — {price}₽, {d}мин\n\nДобавить ещё?", reply_markup={"inline_keyboard": [[{"text": "➕ Да", "callback_data": "onboarding_add_more"}], [{"text": "➡️ Дальше", "callback_data": "onboarding_next"}]]})
    if state == "manual_name": return handle_manual_name(chat_id, text)
    if state == "manual_phone": return handle_manual_phone(chat_id, text)
    if state == "finding_master": return handle_find_master(chat_id, text)
    
    if text == "🔄 Я клиент" and master:
        DB.delete("masters", str(chat_id))
        States.clear(chat_id)
        return TG.send(chat_id, "Роль сброшена. Кто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})
    if text == "🔄 Я мастер" and client:
        DB.delete("clients", str(chat_id))
        States.clear(chat_id)
        return TG.send(chat_id, "Роль сброшена. Кто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})
    
    if text == "👤 Я мастер": return TG.send(chat_id, "Вы уже зарегистрированы!", reply_markup=KBD.master_main()) if master and master.get("completed_onboarding") else register_master(chat_id, user_name, username)
    if text == "👥 Я клиент":
        if not client: DB.set("clients", str(chat_id), {"created_at": now().isoformat()})
        return TG.send(chat_id, 
            "👥 *Добро пожаловать в Мастерилку!*\n\n"
            "🔗 *Запишитесь к мастеру в 3 клика* — по ссылке или номеру телефона\n"
            "📋 *Все ваши записи всегда под рукой*\n"
            "⭐ *Оценивайте работу мастера* — помогайте другим выбирать лучших\n\n"
            "*Выберите действие:*",
            reply_markup=KBD.client_main())
    if text == "📊 Сегодня" and master:
        today = today_str()
        apps = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
        svcs = master.get("services", [])
        ta = [a for a in apps if a.get("date") == today and a.get("status") != "cancelled"]
        total = sum(next((s.get("price",0) for s in svcs if isinstance(s, dict) and s.get("name") == a.get("service")), 0) for a in ta)
        text = f"📊 *Сегодня ({today}):*\n\n📅 Записей: {len(ta)}\n💰 Доход: {total}₽"
        if ta:
            text += "\n\n"
            ta.sort(key=lambda a: a.get("time",""))
            for a in ta[:10]: text += f"• {a.get('time')} — {a.get('client_name','?')} ({a.get('service')})\n"
        return TG.send(chat_id, text, reply_markup=KBD.master_main())
    if text == "📅 Расписание" and master: return show_schedule(chat_id)
    if text == "➕ Новая запись" and master: return start_manual_booking(chat_id)
    if text == "👥 Клиенты" and master: return show_clients(chat_id)
    if text == "🔗 Моя ссылка" and master: return show_master_link(chat_id)
    if text == "🔗 Записаться по ссылке": return handle_client_booking_by_link(chat_id)
    if text == "📤 Поделиться ссылкой": return handle_share_link(chat_id)
    if text == "⚙️ Настройки" and master: return TG.send(chat_id, "⚙️ *Настройки*", reply_markup=KBD.settings())
    if text == "💈 Услуги" and master: return handle_services_settings(chat_id)
    if text == "⏰ Часы работы" and master: return TG.send(chat_id, "⏰ *Часы*", reply_markup=KBD.days_schedule(master))
    if text == "📍 Адрес" and master: return start_set_address(chat_id)
    if text == "🚷 Чёрный список" and master: return show_blacklist(chat_id)
    if text == "📢 Свободные окна" and master: return show_free_slots(chat_id)
    if text == "🖼 Портфолио" and master: States.set(chat_id, {"state": "adding_portfolio"}); return TG.send(chat_id, "🖼 Отправьте фото.")
    if text == "🔙 В меню" and master: States.clear(chat_id); return TG.send(chat_id, "Главное меню", reply_markup=KBD.master_main())
    if text == "📋 Мои записи": return handle_client_appointments(chat_id)
    if text == "🔍 Найти мастера": States.set(chat_id, {"state": "finding_master"}); return TG.send(chat_id, "🔍 Номер:", reply_markup=KBD.cancel())
    if text == "❓ Помощь": return TG.send(chat_id, "📖 *Помощь*\n\n📊 *Сегодня* — сводка\n📅 *Расписание* — записи\n➕ *Новая запись* — вручную\n👥 *Клиенты* — база\n🔗 *Моя ссылка* — клиентам\n⚙️ *Настройки* — услуги, часы\n🔄 *Я клиент/Я мастер* — сменить роль" if master else "📖 *Помощь*\n\n📋 *Мои записи*\n🔗 *Записаться по ссылке*\n📤 *Поделиться ссылкой*\n🔍 *Найти мастера*\n🔄 *Я мастер* — стать мастером")

def handle_callback(chat_id, data):
    if data == "onboarding_skip":
        States.clear(chat_id)
        m = DB.get("masters", str(chat_id))
        step = m.get("onboarding_step", 1) if m else 1
        if step == 1: return onboarding_step_2(chat_id)
        if step == 3: return onboarding_step_4(chat_id)
        return onboarding_step_2(chat_id)
    if data == "onboarding_next": return onboarding_step_2(chat_id)
    if data == "onboarding_add_more": States.set(chat_id, {"state": "onboarding_services"}); return TG.send(chat_id, "✏️ Название:")
    if data == "onboarding_finish": return finish_onboarding(chat_id)
    if data == "restart_onboarding": return start_onboarding(chat_id)
    if data == "back_to_step2": return onboarding_step_2(chat_id)
    if data == "back_to_step3": return onboarding_step_3(chat_id)
    if data == "addservice": return start_add_service(chat_id)
    if data.startswith("delservice_"): return delete_service(chat_id, data.replace("delservice_",""))
    if data == "settings_back": States.clear(chat_id); return TG.send(chat_id, "⚙️ *Настройки*", reply_markup=KBD.settings())
    if data == "add_blacklist": return start_add_blacklist(chat_id)
    if data.startswith("remove_blacklist_"): return handle_remove_blacklist(chat_id, data.replace("remove_blacklist_",""))
    if data == "setall_weekdays": return handle_set_all_weekdays(chat_id)
    if data.startswith("setday_"): return handle_set_day_schedule(chat_id, data.replace("setday_",""))
    if data.startswith("setdayvalue_"):
        parts = data.replace("setdayvalue_","").split("_",1)
        return handle_set_day_value(chat_id, parts[0], parts[1])
    if data == "back_to_days":
        States.clear(chat_id)
        return TG.send(chat_id, "⏰ *Дни:*", reply_markup=KBD.days_schedule(DB.get("masters", str(chat_id))))
    if data == "booking_cancel": States.clear(chat_id); return TG.send(chat_id, "❌ Отменено", reply_markup=KBD.client_main())
    if data == "booking_back_to_svc":
        svcs = States.get(chat_id).get("services", [])
        if not svcs: return TG.send(chat_id, "❌ Сессия истекла.")
        return TG.send(chat_id, "💈 Услуги:", reply_markup={"inline_keyboard": [[{"text": f"{s['name']} — {s['price']}₽", "callback_data": f"bkservice_{s['name']}"}] for s in svcs]})
    if data.startswith("bkservice_"): return handle_booking_service(chat_id, data.replace("bkservice_",""))
    if data.startswith("bkdate_"): return handle_booking_date(chat_id, data.replace("bkdate_",""))
    if data.startswith("bktime_"): return handle_booking_time(chat_id, data.replace("bktime_",""))
    if data.startswith("bkconfirm_"): return handle_booking_confirm(chat_id, data.replace("bkconfirm_",""))
    if data.startswith("manservice_"): return handle_manual_service(chat_id, data.replace("manservice_",""))
    if data.startswith("mandate_"): return handle_manual_date(chat_id, data.replace("mandate_",""))
    if data.startswith("mantime_"): return handle_manual_time(chat_id, data.replace("mantime_",""))
    if data.startswith("schedule_filter_"): return show_schedule(chat_id, data.replace("schedule_filter_",""))
    if data.startswith("freeslots_"): return show_free_slots_day(chat_id, data.replace("freeslots_",""))
    if data.startswith("cancel_"): return handle_cancel_appointment(chat_id, data.replace("cancel_",""))
    if data.startswith("complete_"): return handle_complete_appointment(chat_id, data.replace("complete_",""))
    if data.startswith("noshow_"): return handle_noshow_appointment(chat_id, data.replace("noshow_",""))
    if data.startswith("delete_"): return handle_master_delete_appointment(chat_id, data.replace("delete_",""))
    if data.startswith("reschedule_"): return handle_reschedule_start(chat_id, data.replace("reschedule_",""))
    if data.startswith("res_date_"):
        parts = data.replace("res_date_","").split("_",1)
        return handle_reschedule_date(chat_id, parts[0], parts[1])
    if data.startswith("res_time_"):
        parts = data.replace("res_time_","").split("_",2)
        return handle_reschedule_time(chat_id, parts[0], parts[1], parts[2])
    if data.startswith("cl_reschedule_"): return handle_client_reschedule_start(chat_id, data.replace("cl_reschedule_",""))
    if data.startswith("cl_res_date_"):
        parts = data.replace("cl_res_date_","").split("_",1)
        return handle_client_reschedule_date(chat_id, parts[0], parts[1])
    if data.startswith("cl_res_time_"):
        parts = data.replace("cl_res_time_","").split("_",2)
        return handle_client_reschedule_time(chat_id, parts[0], parts[1], parts[2])
    if data.startswith("rate_"):
        parts = data.replace("rate_","").split("_",1)
        m = DB.get("masters", parts[0])
        if m:
            r, c = m.get("rating",0), m.get("ratings_count",0)
            DB.set("masters", parts[0], {"rating": int((r*c+int(parts[1]))/(c+1)), "ratings_count": c+1})
        TG.send(chat_id, "⭐ Спасибо!", reply_markup=KBD.client_main())
    if data.startswith("add_note_"):
        phone = data.replace("add_note_","")
        States.set(chat_id, {"state": "adding_note", "note_phone": phone})
        return TG.send(chat_id, "📝 Заметка:", reply_markup=KBD.cancel())
    if data.startswith("edit_tags_"):
        phone = data.replace("edit_tags_","")
        return TG.send(chat_id, f"🏷 Теги для {phone}:", reply_markup={"inline_keyboard": [
            [{"text": "🏆 VIP", "callback_data": f"tag_{phone}_VIP"}],
            [{"text": "🔄 Постоянный", "callback_data": f"tag_{phone}_Постоянный"}],
            [{"text": "⚠️ Проблемный", "callback_data": f"tag_{phone}_Проблемный"}],
            [{"text": "🗑 Сбросить", "callback_data": f"tag_{phone}_"}]
        ]})
    if data.startswith("tag_"):
        parts = data.replace("tag_","").split("_",1)
        master = DB.get("masters", str(chat_id))
        tags = master.get("client_tags", {}) if master else {}
        tags[parts[0]] = parts[1] if parts[1] else ""
        DB.set("masters", str(chat_id), {"client_tags": tags})
        return TG.send(chat_id, "✅ Тег сохранён!", reply_markup=KBD.master_main())
    if data.startswith("client_card_"): return show_client_card(chat_id, data.replace("client_card_",""))
    if data == "ignore": pass

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            cl = int(self.headers.get('Content-Length', 0))
            if cl:
                update = json.loads(self.rfile.read(cl).decode('utf-8'))
                self._process(update)
            self._respond(200, {"status": "ok"})
        except Exception as e:
            print(f"ERROR: {e}\n{traceback.format_exc()}")
            self._respond(200, {"status": "error"})
    
    def do_GET(self):
        self._respond(200, {"status": "bot online"})
    
    def _respond(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def _process(self, update):
        if "message" in update:
            msg = update["message"]
            chat_id = str(msg["chat"]["id"])
            user_name = msg["from"].get("first_name", "Пользователь")
            if "photo" in msg:
                if States.get(chat_id).get("state") == "adding_portfolio":
                    master = DB.get("masters", str(chat_id))
                    portfolio = master.get("portfolio", []) if master else []
                    if len(portfolio) >= 5: TG.send(chat_id, "❌ Максимум 5 фото.")
                    else:
                        portfolio.append({"file_id": msg["photo"][-1]["file_id"], "caption": ""})
                        DB.set("masters", str(chat_id), {"portfolio": portfolio})
                        TG.send(chat_id, f"✅ Фото добавлено! ({len(portfolio)}/5)")
                return
            text = msg.get("text", "")
            if text.startswith("/start"):
                if "master_" in text:
                    handle_client_booking_start(chat_id, text.split("master_")[1].split()[0])
                else:
                    handle_start(chat_id, user_name)
            else:
                handle_text(chat_id, user_name, msg["from"].get("username", ""), text)
        elif "callback_query" in update:
            cb = update["callback_query"]
            TG.answer_callback(cb["id"])
            handle_callback(str(cb["message"]["chat"]["id"]), cb.get("data", ""))

app = handler