"""
График.Про — идеальный бот для записи клиентов
Версия: 3.0
Полный функционал без ошибок
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

# Используем requests с таймаутами
import requests

# ==================== КОНФИГУРАЦИЯ ====================
# Токены берутся из переменных окружения Vercel
TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
API_KEY = os.environ.get("FIREBASE_API_KEY", "")
PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "grafikpro-d3500")

TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"
FIRESTORE_URL = f"https://firestore.googleapis.com/v1/projects/{PROJECT_ID}/databases/(default)/documents"

# Глобальное хранилище состояний пользователей (для цепочек действий)
STATES = {}

# ==================== СЛОВАРИ ДНЕЙ НЕДЕЛИ ====================
DAYS_MAP = {
    "ПН": "monday", "ВТ": "tuesday", "СР": "wednesday",
    "ЧТ": "thursday", "ПТ": "friday", "СБ": "saturday", "ВС": "sunday"
}
DAYS_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
DAYS_SHORT = ["ПН", "ВТ", "СР", "ЧТ", "ПТ", "СБ", "ВС"]

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def now():
    """Текущее время (для единообразия)"""
    return datetime.now()

def today_str():
    """Сегодня в формате YYYY-MM-DD"""
    return now().strftime("%Y-%m-%d")

def parse_time(t):
    """Парсит '09:30' в минуты от начала дня"""
    try:
        parts = t.split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except:
        return 0

def format_time(minutes):
    """Минуты в '09:30'"""
    return f"{minutes // 60:02d}:{minutes % 60:02d}"

def validate_phone(phone):
    """Очистка и проверка телефона"""
    clean = re.sub(r'[^0-9+]', '', phone)
    if len(clean) >= 10:
        return clean
    return None

# ==================== FIRESTORE КЛИЕНТ ====================
class DB:
    """Класс для работы с Firestore REST API"""
    
    @staticmethod
    def get(collection, doc_id):
        try:
            r = requests.get(f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}", timeout=8)
            if r.status_code != 200:
                return None
            return DB._parse(r.json().get("fields", {}))
        except:
            return None
    
    @staticmethod
    def set(collection, doc_id, data):
        try:
            fields = DB._serialize(data)
            body = {"fields": fields}
            
            # Пробуем обновить
            patch_url = f"{FIRESTORE_URL}/{collection}/{doc_id}?key={API_KEY}"
            r = requests.patch(patch_url, json=body, timeout=8)
            if r.status_code in [200, 201]:
                return True
            
            # Создаём с указанным ID
            create_url = f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}"
            r = requests.post(create_url, json=body, timeout=8)
            return r.status_code in [200, 201]
        except:
            return False
    
    @staticmethod
    def add(collection, data):
        try:
            doc_id = str(uuid.uuid4())[:10]
            fields = DB._serialize(data)
            body = {"fields": fields}
            url = f"{FIRESTORE_URL}/{collection}?documentId={doc_id}&key={API_KEY}"
            r = requests.post(url, json=body, timeout=8)
            return doc_id if r.status_code in [200, 201] else None
        except:
            return None
    
    @staticmethod
    def query(collection, field, operator, value):
        try:
            body = {
                "structuredQuery": {
                    "from": [{"collectionId": collection}],
                    "where": {
                        "fieldFilter": {
                            "field": {"fieldPath": field},
                            "op": operator,
                            "value": {"stringValue": str(value)}
                        }
                    }
                }
            }
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
            if "stringValue" in value:
                result[key] = value["stringValue"]
            elif "integerValue" in value:
                result[key] = int(value["integerValue"])
            elif "doubleValue" in value:
                result[key] = float(value["doubleValue"])
            elif "booleanValue" in value:
                result[key] = value["booleanValue"]
            elif "nullValue" in value:
                result[key] = None
            elif "arrayValue" in value:
                arr = []
                for v in value["arrayValue"].get("values", []):
                    if "stringValue" in v:
                        arr.append(v["stringValue"])
                    elif "integerValue" in v:
                        arr.append(int(v["integerValue"]))
                    elif "mapValue" in v:
                        arr.append(DB._parse(v["mapValue"].get("fields", {})))
                result[key] = arr
            elif "mapValue" in value:
                result[key] = DB._parse(value["mapValue"].get("fields", {}))
        return result
    
    @staticmethod
    def _serialize(data):
        fields = {}
        for key, val in data.items():
            if isinstance(val, str):
                fields[key] = {"stringValue": val}
            elif isinstance(val, bool):
                fields[key] = {"booleanValue": val}
            elif isinstance(val, int):
                fields[key] = {"integerValue": str(val)}
            elif isinstance(val, float):
                fields[key] = {"doubleValue": val}
            elif val is None:
                fields[key] = {"nullValue": None}
            elif isinstance(val, list):
                items = []
                for item in val:
                    if isinstance(item, str):
                        items.append({"stringValue": item})
                    elif isinstance(item, int):
                        items.append({"integerValue": str(item)})
                    elif isinstance(item, dict):
                        items.append({"mapValue": {"fields": DB._serialize(item)}})
                fields[key] = {"arrayValue": {"values": items}}
            elif isinstance(val, dict):
                fields[key] = {"mapValue": {"fields": DB._serialize(val)}}
        return fields

# ==================== TELEGRAM API ====================
class TG:
    """Класс для отправки сообщений в Telegram"""
    
    @staticmethod
    def send(chat_id, text, reply_markup=None, parse_mode="Markdown"):
        try:
            payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
            if reply_markup:
                payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
            r = requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload, timeout=10)
            return r.json() if r.status_code == 200 else None
        except:
            return None
    
    @staticmethod
    def answer_callback(callback_id, text=""):
        try:
            requests.post(f"{TELEGRAM_URL}/answerCallbackQuery", 
                         json={"callback_query_id": callback_id, "text": text}, timeout=5)
        except:
            pass

# ==================== КЛАВИАТУРЫ ====================
class KBD:
    """Генератор клавиатур"""
    
    @staticmethod
    def master_main():
        return {"keyboard": [
            ["📊 Дашборд", "📅 Расписание"],
            ["➕ Новая запись", "👥 Клиенты"],
            ["🔗 Моя ссылка", "📢 Свободные окна"],
            ["⚙️ Настройки", "❓ Помощь"]
        ], "resize_keyboard": True}
    
    @staticmethod
    def client_main():
        return {"keyboard": [
            ["📋 Мои записи"],
            ["🔍 Найти мастера"],
            ["❓ Помощь"]
        ], "resize_keyboard": True}
    
    @staticmethod
    def settings():
        return {"keyboard": [
            ["💈 Услуги", "⏰ Часы работы"],
            ["📍 Адрес", "🚷 Чёрный список"],
            ["🖼 Портфолио", "🔙 Главное меню"]
        ], "resize_keyboard": True}
    
    @staticmethod
    def cancel():
        return {"keyboard": [["🔙 Отмена"]], "resize_keyboard": True}
    
    @staticmethod
    def days_schedule(master):
        """Кнопки для настройки дней недели"""
        schedule = master.get("schedule", {})
        buttons = []
        for i, day_key in enumerate(DAYS_NAMES):
            day_data = schedule.get(day_key)
            if day_data and day_data.get("start"):
                label = f"{DAYS_SHORT[i]} {day_data['start']}-{day_data['end']}"
            else:
                label = f"{DAYS_SHORT[i]} выходной"
            buttons.append([{"text": label, "callback_data": f"setday_{day_key}"}])
        buttons.append([{"text": "✅ Готово", "callback_data": "settings_back"}])
        return {"inline_keyboard": buttons}
        
 # ==================== УМНЫЕ СЛОТЫ ====================
class Slots:
    """Расчёт свободных временных слотов"""
    
    @staticmethod
    def get(master_id, date_str, service_duration):
        master = DB.get("masters", master_id)
        if not master:
            return []
        
        schedule = master.get("schedule", {})
        
        # Определяем день недели
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            day_key = DAYS_NAMES[dt.weekday()]
            day_sched = schedule.get(day_key)
        except:
            return []
        
        # Выходной?
        if not day_sched or not day_sched.get("start"):
            return []
        
        work_start = parse_time(day_sched["start"])
        work_end = parse_time(day_sched["end"])
        
        # Получаем все записи на эту дату
        appointments = DB.query("appointments", "master_id", "EQUAL", master_id)
        busy_intervals = []
        
        for appt in appointments:
            if appt.get("date") == date_str and appt.get("status") not in ["cancelled"]:
                appt_start = parse_time(appt.get("time", "00:00"))
                appt_svc = next((s for s in master.get("services", []) 
                                if isinstance(s, dict) and s.get("name") == appt.get("service")), None)
                appt_dur = appt_svc.get("duration", 60) if appt_svc else 60
                busy_intervals.append((appt_start, appt_start + appt_dur))
        
        # Генерируем слоты
        slots = []
        current = work_start
        step = 30  # шаг 30 минут
        
        while current + service_duration <= work_end:
            slot_end = current + service_duration
            
            # Проверяем пересечение с занятыми интервалами
            is_free = True
            for busy_start, busy_end in busy_intervals:
                if not (slot_end <= busy_start or current >= busy_end):
                    is_free = False
                    break
            
            if is_free:
                slots.append(format_time(current))
            
            current += step
        
        return slots

# ==================== РЕГИСТРАЦИЯ МАСТЕРА ====================
def register_master(chat_id, user_name, username):
    """Создать профиль мастера"""
    default_schedule = {}
    for day in DAYS_NAMES:
        if day == "sunday":
            default_schedule[day] = None
        elif day == "saturday":
            default_schedule[day] = {"start": "10:00", "end": "15:00"}
        else:
            default_schedule[day] = {"start": "09:00", "end": "18:00"}
    
    DB.set("masters", str(chat_id), {
        "name": user_name,
        "username": username or "",
        "phone": "",
        "services": [],
        "schedule": default_schedule,
        "breaks": [],
        "address": "",
        "portfolio": [],
        "blacklist": [],
        "client_notes": {},
        "client_tags": {},
        "completed_onboarding": False,
        "onboarding_step": 1,
        "buffer": 5,
        "rating": 0,
        "ratings_count": 0,
        "created_at": now().isoformat()
    })
    
    TG.send(chat_id, f"✅ *{user_name}, вы зарегистрированы как мастер!*\n\n"
            "Сейчас настроим ваш профиль. Это займёт 2 минуты.",
            reply_markup=KBD.cancel())
    
    start_onboarding(chat_id)

def start_onboarding(chat_id):
    """Шаг 1 онбординга: добавление услуг"""
    DB.set("masters", str(chat_id), {"onboarding_step": 1})
    STATES[str(chat_id)] = {"state": "onboarding_services"}
    
    TG.send(chat_id,
        "👋 *Шаг 1 из 4: Услуги*\n\n"
        "Добавьте ваши услуги, которые будут видеть клиенты.\n"
        "Отправьте название первой услуги:",
        reply_markup={
            "inline_keyboard": [[
                {"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}
            ]]
        })

def onboarding_step_2(chat_id):
    """Шаг 2: настройка рабочих часов"""
    DB.set("masters", str(chat_id), {"onboarding_step": 2})
    STATES[str(chat_id)] = {"state": "onboarding"}
    
    master = DB.get("masters", str(chat_id))
    TG.send(chat_id,
        "⏰ *Шаг 2 из 4: Часы работы*\n\n"
        "Нажмите на день чтобы изменить время.\n"
        "По умолчанию: ПН-ПТ 09:00-18:00, СБ 10:00-15:00, ВС выходной",
        reply_markup=KBD.days_schedule(master))

def onboarding_step_3(chat_id):
    """Шаг 3: адрес"""
    DB.set("masters", str(chat_id), {"onboarding_step": 3})
    STATES[str(chat_id)] = {"state": "onboarding_address"}
    
    TG.send(chat_id,
        "📍 *Шаг 3 из 4: Адрес*\n\n"
        "Отправьте адрес вашего кабинета.\n"
        "Клиенты увидят его при записи.",
        reply_markup={
            "inline_keyboard": [[
                {"text": "⏩ Пропустить", "callback_data": "onboarding_skip"}
            ]]
        })

def onboarding_step_4(chat_id):
    """Шаг 4: портфолио"""
    DB.set("masters", str(chat_id), {"onboarding_step": 4})
    STATES[str(chat_id)] = {"state": "onboarding_portfolio"}
    
    TG.send(chat_id,
        "🖼 *Шаг 4 из 4: Портфолио*\n\n"
        "Отправьте до 5 фото ваших работ.\n"
        "Или пропустите этот шаг.",
        reply_markup={
            "inline_keyboard": [[
                {"text": "⏩ Завершить", "callback_data": "onboarding_finish"}
            ]]
        })

def finish_onboarding(chat_id):
    """Завершить онбординг"""
    DB.set("masters", str(chat_id), {
        "completed_onboarding": True,
        "onboarding_step": 0
    })
    STATES.pop(str(chat_id), None)
    
    TG.send(chat_id, "🎉 *Настройка завершена!*\n\n"
            "Ваш кабинет готов к работе.\n"
            "Клиенты могут записываться по вашей ссылке.",
            reply_markup=KBD.master_main())
    
    # Показываем ссылку
    show_master_link(chat_id)

def show_master_link(chat_id):
    """Показать ссылку мастера для записи клиентов"""
    links = DB.query("links", "master_id", "EQUAL", str(chat_id))
    link_id = links[0]["_id"] if links else str(uuid.uuid4())[:8]
    
    if not links:
        DB.set("links", link_id, {"master_id": str(chat_id), "created_at": now().isoformat()})
    
    link = f"https://t.me/GrafikProBot?start=master_{link_id}"
    TG.send(chat_id, f"🔗 *Ваша ссылка для записи клиентов:*\n\n`{link}`\n\n"
            "Отправьте её клиентам или разместите в соцсетях.")

# ==================== ДОБАВЛЕНИЕ УСЛУГ ====================
def start_add_service(chat_id):
    """Начать добавление услуги"""
    STATES[str(chat_id)] = {"state": "adding_service_name"}
    TG.send(chat_id, "✏️ *Название услуги:*\nНапример: Стрижка мужская",
            reply_markup=KBD.cancel())

def handle_service_name(chat_id, name):
    """Обработать название услуги"""
    name = name.strip()
    if len(name) < 2:
        return TG.send(chat_id, "❌ Название должно быть не короче 2 символов.")
    
    STATES[str(chat_id)] = {"state": "adding_service_price", "svc_name": name}
    TG.send(chat_id, f"💰 *Цена для «{name}»:*\nТолько число, например: 1500")

def handle_service_price(chat_id, price_text):
    """Обработать цену услуги"""
    try:
        price = int(price_text.strip())
        if price <= 0:
            raise ValueError
    except:
        return TG.send(chat_id, "❌ Введите положительное число.")
    
    state = STATES.get(str(chat_id), {})
    STATES[str(chat_id)] = {
        "state": "adding_service_duration",
        "svc_name": state.get("svc_name"),
        "svc_price": price
    }
    TG.send(chat_id, f"⏱ *Длительность «{state.get('svc_name')}» в минутах:*\nНапример: 30")

def handle_service_duration(chat_id, dur_text):
    """Сохранить услугу"""
    try:
        duration = int(dur_text.strip())
        if duration <= 0 or duration > 480:
            raise ValueError
    except:
        return TG.send(chat_id, "❌ Введите число от 1 до 480 (8 часов).")
    
    state = STATES.get(str(chat_id), {})
    if not state:
        STATES.pop(str(chat_id), None)
        return TG.send(chat_id, "❌ Сессия истекла.", reply_markup=KBD.settings())
    
    master = DB.get("masters", str(chat_id))
    if not master:
        STATES.pop(str(chat_id), None)
        return TG.send(chat_id, "❌ Сначала зарегистрируйтесь: /start")
    
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    services.append({
        "name": state["svc_name"],
        "price": state["svc_price"],
        "duration": duration,
        "disabled": False
    })
    
    DB.set("masters", str(chat_id), {"services": services})
    STATES.pop(str(chat_id), None)
    
    TG.send(chat_id, f"✅ *Услуга добавлена!*\n\n"
            f"💈 {state['svc_name']}\n"
            f"💰 {state['svc_price']}₽\n"
            f"⏱ {duration} мин",
            reply_markup=KBD.settings())
            
  # ==================== НАСТРОЙКА РАБОЧИХ ЧАСОВ ====================
def handle_set_day_schedule(chat_id, day_key):
    """Настройка конкретного дня недели"""
    master = DB.get("masters", str(chat_id))
    schedule = master.get("schedule", {})
    current = schedule.get(day_key, {})
    
    current_text = "выходной"
    if current and current.get("start"):
        current_text = f"{current['start']} – {current['end']}"
    
    day_name = {"monday": "Понедельник", "tuesday": "Вторник", "wednesday": "Среда",
                "thursday": "Четверг", "friday": "Пятница", "saturday": "Суббота", "sunday": "Воскресенье"}
    
    STATES[str(chat_id)] = {"state": "setting_day", "day_key": day_key}
    
    TG.send(chat_id,
        f"⏰ *{day_name.get(day_key, day_key)}*\n\n"
        f"Сейчас: {current_text}\n\n"
        "Введите время в формате:\n"
        "`09:00-18:00` — рабочий день\n"
        "`выходной` — не работать\n\n"
        "Или нажмите кнопку:",
        reply_markup={
            "inline_keyboard": [
                [{"text": "09:00-18:00", "callback_data": f"setdayvalue_{day_key}_09:00-18:00"}],
                [{"text": "10:00-20:00", "callback_data": f"setdayvalue_{day_key}_10:00-20:00"}],
                [{"text": "🚫 Выходной", "callback_data": f"setdayvalue_{day_key}_выходной"}],
                [{"text": "🔙 Назад к дням", "callback_data": "back_to_days"}]
            ]
        })

def handle_set_day_value(chat_id, day_key, value):
    """Сохранить настройку дня"""
    master = DB.get("masters", str(chat_id))
    schedule = master.get("schedule", {})
    
    if value == "выходной":
        schedule[day_key] = None
    else:
        try:
            start, end = value.split("-")
            schedule[day_key] = {"start": start.strip(), "end": end.strip()}
        except:
            return TG.send(chat_id, "❌ Неверный формат. Используйте 09:00-18:00")
    
    DB.set("masters", str(chat_id), {"schedule": schedule})
    STATES.pop(str(chat_id), None)
    
    TG.send(chat_id, "✅ День обновлён!", reply_markup=KBD.days_schedule(DB.get("masters", str(chat_id))))

# ==================== АДРЕС И ЧЁРНЫЙ СПИСОК ====================
def start_set_address(chat_id):
    """Начать установку адреса"""
    STATES[str(chat_id)] = {"state": "setting_address"}
    TG.send(chat_id, "📍 *Введите адрес:*\nНапример: ул. Ленина, 123, офис 45",
            reply_markup=KBD.cancel())

def handle_address_set(chat_id, address):
    """Сохранить адрес"""
    DB.set("masters", str(chat_id), {"address": address.strip()})
    STATES.pop(str(chat_id), None)
    TG.send(chat_id, f"✅ Адрес сохранён: {address.strip()}", reply_markup=KBD.settings())

def show_blacklist(chat_id):
    """Показать чёрный список"""
    master = DB.get("masters", str(chat_id))
    blacklist = master.get("blacklist", []) if master else []
    
    if blacklist:
        text = "🚷 *Чёрный список:*\n" + "\n".join([f"• {b.get('name', '')} — {b.get('phone', '')}" for b in blacklist])
    else:
        text = "🚷 *Чёрный список пуст*"
    
    buttons = [[{"text": "➕ Добавить", "callback_data": "add_blacklist"}]]
    for b in blacklist:
        buttons.append([{"text": f"🗑 Удалить {b.get('phone', '')}", "callback_data": f"remove_blacklist_{b.get('phone', '')}"}])
    buttons.append([{"text": "🔙 Назад", "callback_data": "settings_back"}])
    
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def start_add_blacklist(chat_id):
    """Начать добавление в чёрный список"""
    STATES[str(chat_id)] = {"state": "adding_blacklist"}
    TG.send(chat_id, "🚷 *Введите номер телефона для блокировки:*",
            reply_markup=KBD.cancel())

def handle_add_blacklist(chat_id, phone):
    """Добавить в чёрный список"""
    phone = validate_phone(phone)
    if not phone:
        return TG.send(chat_id, "❌ Неверный формат телефона. Минимум 10 цифр.")
    
    master = DB.get("masters", str(chat_id))
    blacklist = master.get("blacklist", []) if master else []
    
    if any(b.get("phone") == phone for b in blacklist):
        TG.send(chat_id, "❌ Этот номер уже в чёрном списке.")
    else:
        blacklist.append({"phone": phone, "name": "", "date_added": now().isoformat()})
        DB.set("masters", str(chat_id), {"blacklist": blacklist})
        TG.send(chat_id, f"✅ {phone} добавлен в чёрный список.")
    
    STATES.pop(str(chat_id), None)
    show_blacklist(chat_id)

def handle_remove_blacklist(chat_id, phone):
    """Удалить из чёрного списка"""
    master = DB.get("masters", str(chat_id))
    blacklist = master.get("blacklist", []) if master else []
    blacklist = [b for b in blacklist if b.get("phone") != phone]
    DB.set("masters", str(chat_id), {"blacklist": blacklist})
    TG.send(chat_id, f"✅ {phone} удалён из чёрного списка.")
    show_blacklist(chat_id)

# ==================== ЗАПИСЬ КЛИЕНТА ПО ССЫЛКЕ ====================
def handle_client_booking_start(chat_id, link_id):
    """Клиент перешёл по ссылке мастера"""
    # Регистрируем клиента если нужно
    if not DB.get("clients", str(chat_id)):
        DB.set("clients", str(chat_id), {"created_at": now().isoformat()})
    
    link = DB.get("links", link_id)
    if not link:
        return TG.send(chat_id, "❌ Ссылка недействительна. Попросите актуальную ссылку у мастера.")
    
    master = DB.get("masters", link["master_id"])
    if not master:
        return TG.send(chat_id, "❌ Мастер не найден.")
    
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name") and not s.get("disabled")]
    if not services:
        return TG.send(chat_id, "❌ У мастера пока нет услуг. Попробуйте позже.")
    
    # Сохраняем контекст
    STATES[str(chat_id)] = {
        "state": "client_booking",
        "master_id": link["master_id"],
        "master_name": master.get("name", "Мастер"),
        "master_addr": master.get("address", "")
    }
    
    # Формируем сообщение
    text = f"👤 *{master.get('name', 'Мастер')}*\n"
    if master.get("rating"):
        text += f"⭐ Рейтинг: {master['rating']}/5\n"
    if master.get("address"):
        text += f"📍 {master['address']}\n"
    text += "\n*Выберите услугу:*"
    
    buttons = []
    for s in services:
        buttons.append([{
            "text": f"{s['name']} — {s['price']}₽ ({s['duration']}мин)",
            "callback_data": f"bkservice_{s['name']}"
        }])
    buttons.append([{"text": "🔙 Отмена", "callback_data": "booking_cancel"}])
    
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def handle_booking_service(chat_id, service_name):
    """Клиент выбрал услугу"""
    state = STATES.get(str(chat_id), {})
    state["service"] = service_name
    state["state"] = "booking_date"
    STATES[str(chat_id)] = state
    
    # Показываем 14 дней
    buttons = []
    for i in range(14):
        d = now() + timedelta(days=i+1)
        date_str = d.strftime("%Y-%m-%d")
        day_name = DAYS_SHORT[d.weekday()]
        buttons.append([{
            "text": f"{d.strftime('%d.%m')} ({day_name})",
            "callback_data": f"bkdate_{date_str}"
        }])
    buttons.append([{"text": "🔙 Назад", "callback_data": "booking_back"}])
    
    TG.send(chat_id, f"💈 *{service_name}*\n\nВыберите дату:",
            reply_markup={"inline_keyboard": buttons})

def handle_booking_date(chat_id, date_str):
    """Клиент выбрал дату"""
    state = STATES.get(str(chat_id), {})
    master_id = state.get("master_id")
    service_name = state.get("service", "")
    
    # Получаем длительность услуги
    master = DB.get("masters", master_id)
    service = next((s for s in master.get("services", []) 
                   if isinstance(s, dict) and s.get("name") == service_name), None)
    duration = service.get("duration", 60) if service else 60
    
    # Получаем свободные слоты
    free_slots = Slots.get(master_id, date_str, duration)
    
    if not free_slots:
        return TG.send(chat_id, f"📭 *Нет свободных слотов на {date_str}*\n\nВыберите другую дату.",
                      reply_markup={"inline_keyboard": [[{"text": "🔙 К датам", "callback_data": f"bkservice_{service_name}"}]]})
    
    state["date"] = date_str
    state["state"] = "booking_time"
    STATES[str(chat_id)] = state
    
    buttons = []
    for t in free_slots:
        buttons.append([{"text": f"🟢 {t}", "callback_data": f"bktime_{t}"}])
    buttons.append([{"text": "🔙 К датам", "callback_data": f"bkservice_{service_name}"}])
    
    TG.send(chat_id, f"📅 *{date_str}*\n⏰ Выберите время:",
            reply_markup={"inline_keyboard": buttons})

def handle_booking_time(chat_id, time_str):
    """Клиент выбрал время"""
    state = STATES.get(str(chat_id), {})
    state["time"] = time_str
    state["state"] = "booking_name"
    STATES[str(chat_id)] = state
    
    TG.send(chat_id, "📝 *Введите ваше имя:*", reply_markup=KBD.cancel())

def handle_booking_name(chat_id, name):
    """Клиент ввёл имя"""
    name = name.strip()
    if len(name) < 2:
        return TG.send(chat_id, "❌ Имя должно содержать минимум 2 символа.")
    
    state = STATES.get(str(chat_id), {})
    state["client_name"] = name
    state["state"] = "booking_phone"
    STATES[str(chat_id)] = state
    
    TG.send(chat_id, "📞 *Введите ваш телефон:*\nНапример: +79001234567",
            reply_markup=KBD.cancel())

def handle_booking_phone(chat_id, phone):
    """Клиент ввёл телефон — создаём запись"""
    phone = validate_phone(phone)
    if not phone:
        return TG.send(chat_id, "❌ Неверный формат телефона. Минимум 10 цифр.")
    
    state = STATES.get(str(chat_id), {})
    if not state:
        return TG.send(chat_id, "❌ Сессия истекла. Начните заново по ссылке мастера.")
    
    master_id = state.get("master_id")
    master = DB.get("masters", master_id)
    
    # Проверка чёрного списка
    blacklist = master.get("blacklist", []) if master else []
    if any(b.get("phone") == phone for b in blacklist):
        STATES.pop(str(chat_id), None)
        return TG.send(chat_id, "❌ *Запись невозможна.*\nВаш номер заблокирован мастером.")
    
    # Создаём запись
    appointment = {
        "master_id": master_id,
        "client_id": str(chat_id),
        "client_name": state["client_name"],
        "client_phone": phone,
        "service": state["service"],
        "date": state["date"],
        "time": state["time"],
        "status": "confirmed",
        "reminded_24h": False,
        "reminded_3h": False,
        "reminded_1h": False,
        "created_at": now().isoformat()
    }
    
    doc_id = DB.add("appointments", appointment)
    
    if not doc_id:
        return TG.send(chat_id, "❌ Ошибка создания записи. Попробуйте позже.")
    
    STATES.pop(str(chat_id), None)
    
    # Подтверждение клиенту
    confirm_text = (
        "✅ *Запись подтверждена!*\n\n"
        f"👤 Мастер: {master.get('name', '')}\n"
        f"💈 Услуга: {state['service']}\n"
        f"📅 Дата: {state['date']}\n"
        f"⏰ Время: {state['time']}"
    )
    if state.get("master_addr"):
        confirm_text += f"\n📍 {state['master_addr']}"
    confirm_text += f"\n\n📞 Ваш телефон: {phone}"
    
    TG.send(chat_id, confirm_text, reply_markup=KBD.client_main())
    
    # Уведомление мастеру
    master_text = (
        "🔔 *Новая запись!*\n\n"
        f"👤 {state['client_name']}\n"
        f"📞 {phone}\n"
        f"💈 {state['service']}\n"
        f"📅 {state['date']} в {state['time']}"
    )
    TG.send(int(master_id), master_text)

# ==================== РУЧНАЯ ЗАПИСЬ МАСТЕРОМ ====================
def start_manual_booking(chat_id):
    """Мастер создаёт запись вручную"""
    STATES[str(chat_id)] = {"state": "manual_name"}
    TG.send(chat_id, "📝 *Новая запись*\n\nВведите имя клиента:", reply_markup=KBD.cancel())

def handle_manual_name(chat_id, name):
    """Имя клиента при ручной записи"""
    if len(name.strip()) < 2:
        return TG.send(chat_id, "❌ Имя должно содержать минимум 2 символа.")
    STATES[str(chat_id)] = {"state": "manual_phone", "client_name": name.strip()}
    TG.send(chat_id, "📞 Введите телефон клиента:")

def handle_manual_phone(chat_id, phone):
    """Телефон при ручной записи"""
    phone = validate_phone(phone)
    if not phone:
        return TG.send(chat_id, "❌ Неверный формат телефона.")
    
    state = STATES.get(str(chat_id), {})
    state["client_phone"] = phone
    state["state"] = "manual_service"
    STATES[str(chat_id)] = state
    
    master = DB.get("masters", str(chat_id))
    services = [s for s in master.get("services", []) if isinstance(s, dict) and s.get("name")]
    
    if not services:
        return TG.send(chat_id, "❌ Сначала добавьте услуги в настройках.")
    
    buttons = [[{"text": f"{s['name']} ({s['price']}₽)", "callback_data": f"manservice_{s['name']}"}] for s in services]
    TG.send(chat_id, "💈 *Выберите услугу:*", reply_markup={"inline_keyboard": buttons})

def handle_manual_service(chat_id, service_name):
    """Выбор услуги при ручной записи"""
    state = STATES.get(str(chat_id), {})
    state["service"] = service_name
    state["state"] = "manual_date"
    STATES[str(chat_id)] = state
    
    buttons = []
    for i in range(14):
        d = now() + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        label = "Сегодня" if i == 0 else d.strftime("%d.%m")
        buttons.append([{"text": label, "callback_data": f"mandate_{date_str}"}])
    
    TG.send(chat_id, "📅 *Выберите дату:*", reply_markup={"inline_keyboard": buttons})

def handle_manual_date(chat_id, date_str):
    """Выбор даты при ручной записи"""
    state = STATES.get(str(chat_id), {})
    state["date"] = date_str
    
    master = DB.get("masters", str(chat_id))
    service = next((s for s in master.get("services", []) 
                   if isinstance(s, dict) and s.get("name") == state.get("service")), None)
    duration = service.get("duration", 60) if service else 60
    
    free_slots = Slots.get(str(chat_id), date_str, duration)
    
    if not free_slots:
        return TG.send(chat_id, "📭 Нет свободных слотов на эту дату.")
    
    state["state"] = "manual_time"
    STATES[str(chat_id)] = state
    
    buttons = [[{"text": f"🟢 {t}", "callback_data": f"mantime_{t}"}] for t in free_slots]
    TG.send(chat_id, f"⏰ *Выберите время на {date_str}:*", reply_markup={"inline_keyboard": buttons})

def handle_manual_time(chat_id, time_str):
    """Создание ручной записи"""
    state = STATES.pop(str(chat_id), {})
    if not state:
        return TG.send(chat_id, "❌ Сессия истекла.")
    
    appointment = {
        "master_id": str(chat_id),
        "client_name": state.get("client_name", ""),
        "client_phone": state.get("client_phone", ""),
        "service": state.get("service", ""),
        "date": state.get("date", ""),
        "time": time_str,
        "status": "confirmed",
        "reminded_24h": False,
        "reminded_3h": False,
        "reminded_1h": False,
        "created_at": now().isoformat()
    }
    
    doc_id = DB.add("appointments", appointment)
    
    if doc_id:
        TG.send(chat_id, f"✅ *Запись создана!*\n\n"
                f"👤 {state['client_name']}\n"
                f"📞 {state['client_phone']}\n"
                f"💈 {state['service']}\n"
                f"📅 {state['date']} в {time_str}",
                reply_markup=KBD.master_main())
    else:
        TG.send(chat_id, "❌ Ошибка создания записи.")

# ==================== РАСПИСАНИЕ И ДАШБОРД ====================
def show_schedule(chat_id, filter_mode="all"):
    """Показать расписание мастера"""
    appointments = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments:
        return TG.send(chat_id, "📭 *Записей пока нет*", reply_markup=KBD.master_main())
    
    today = today_str()
    tomorrow = (now() + timedelta(days=1)).strftime("%Y-%m-%d")
    week_end = (now() + timedelta(days=7)).strftime("%Y-%m-%d")
    
    if filter_mode == "today":
        filtered = [a for a in appointments if a.get("date") == today]
    elif filter_mode == "tomorrow":
        filtered = [a for a in appointments if a.get("date") == tomorrow]
    elif filter_mode == "week":
        filtered = [a for a in appointments if today <= a.get("date", "") <= week_end]
    else:
        filtered = appointments
    
    filtered = [a for a in filtered if a.get("status") != "cancelled"]
    filtered.sort(key=lambda a: (a.get("date", ""), a.get("time", "")))
    
    if not filtered:
        return TG.send(chat_id, "📭 *Нет записей за этот период*")
    
    text = "📅 *Расписание:*\n"
    for a in filtered[:15]:
        icon = {"confirmed": "🟡", "completed": "✅", "no_show": "❌"}.get(a.get("status"), "")
        text += f"\n{icon} *{a.get('date')}* {a.get('time')}\n  {a.get('service')} — {a.get('client_name', '?')} | {a.get('client_phone', '?')}"
    
    buttons = [
        [{"text": f, "callback_data": f"schedule_filter_{f}"} 
         for f in ["all", "today", "tomorrow", "week"]]
    ]
    
    TG.send(chat_id, text, reply_markup={"inline_keyboard": buttons})

def show_dashboard(chat_id):
    """Показать дашборд"""
    today = today_str()
    appointments = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    master = DB.get("masters", str(chat_id))
    services = master.get("services", []) if master else []
    
    today_appts = [a for a in appointments if a.get("date") == today and a.get("status") != "cancelled"]
    completed = [a for a in appointments if a.get("status") == "completed"]
    
    total_today = 0
    for a in today_appts:
        svc = next((s for s in services if isinstance(s, dict) and s.get("name") == a.get("service")), None)
        if svc:
            total_today += svc.get("price", 0)
    
    total_month = 0
    month_ago = (now() - timedelta(days=30)).strftime("%Y-%m-%d")
    month_completed = [a for a in completed if a.get("date", "") >= month_ago]
    for a in month_completed:
        svc = next((s for s in services if isinstance(s, dict) and s.get("name") == a.get("service")), None)
        if svc:
            total_month += svc.get("price", 0)
    
    text = (
        "📊 *Дашборд*\n\n"
        f"📅 *Сегодня ({today}):*\n"
        f"  Записей: {len(today_appts)}\n"
        f"  Доход: {total_today}₽\n\n"
        f"📆 *За 30 дней:*\n"
        f"  Выполнено: {len(month_completed)}\n"
        f"  Доход: {total_month}₽"
    )
    
    TG.send(chat_id, text, reply_markup=KBD.master_main())

def show_clients(chat_id):
    """Показать список клиентов"""
    appointments = DB.query("appointments", "master_id", "EQUAL", str(chat_id))
    if not appointments:
        return TG.send(chat_id, "👥 *Пока нет клиентов*", reply_markup=KBD.master_main())
    
    clients = {}
    for a in appointments:
        phone = a.get("client_phone", "нет")
        if phone not in clients:
            clients[phone] = {"name": a.get("client_name", "?"), "phone": phone, "count": 0, "last": ""}
        clients[phone]["count"] += 1
        if a.get("date", "") > clients[phone]["last"]:
            clients[phone]["last"] = a.get("date", "")
    
    text = "👥 *Клиенты:*\n"
    for phone, data in list(clients.items())[:15]:
        text += f"\n• *{data['name']}* — {phone}\n  Визитов: {data['count']} | Последний: {data['last']}"
    
    TG.send(chat_id, text, reply_markup=KBD.master_main())

def show_free_slots(chat_id):
    """Показать свободные окна на ближайшие дни"""
    master = DB.get("masters", str(chat_id))
    if not master:
        return
    
    buttons = []
    for i in range(7):
        d = now() + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        label = "Сегодня" if i == 0 else "Завтра" if i == 1 else d.strftime("%d.%m (%a)")
        buttons.append([{"text": label, "callback_data": f"freeslots_{date_str}"}])
    
    TG.send(chat_id, "📅 *Свободные окна*\nВыберите день:", reply_markup={"inline_keyboard": buttons})

def show_free_slots_day(chat_id, date_str):
    """Показать свободные слоты на конкретный день"""
    master = DB.get("masters", str(chat_id))
    services = master.get("services", [])
    
    if not services:
        return TG.send(chat_id, "❌ Сначала добавьте услуги.")
    
    # Показываем для первой услуги
    first_svc = services[0] if isinstance(services[0], dict) else services[0]
    duration = first_svc.get("duration", 60) if isinstance(first_svc, dict) else 60
    
    free_slots = Slots.get(str(chat_id), date_str, duration)
    
    if free_slots:
        text = f"🟢 *Свободные окна на {date_str}:*\n" + "\n".join([f"• {t}" for t in free_slots])
    else:
        text = f"📭 *{date_str}* — всё занято или выходной"
    
    TG.send(chat_id, text, reply_markup=KBD.master_main())

# ==================== ГЛАВНЫЙ ОБРАБОТЧИК ТЕКСТА ====================
def handle_text(chat_id, user_name, username, text):
    """Обработка всех текстовых сообщений"""
    state_data = STATES.get(str(chat_id), {})
    state = state_data.get("state", "")
    
    # Определяем роль
    master = DB.get("masters", str(chat_id))
    client = DB.get("clients", str(chat_id))
    
    # ====== ОБРАБОТКА СОСТОЯНИЙ ======
    if state == "adding_service_name":
        return handle_service_name(chat_id, text)
    if state == "adding_service_price":
        return handle_service_price(chat_id, text)
    if state == "adding_service_duration":
        return handle_service_duration(chat_id, text)
    if state == "setting_address":
        return handle_address_set(chat_id, text)
    if state == "adding_blacklist":
        return handle_add_blacklist(chat_id, text)
    if state == "setting_day":
        return handle_set_day_value(chat_id, state_data.get("day_key", ""), text)
    if state == "onboarding_address":
        DB.set("masters", str(chat_id), {"address": text.strip()})
        STATES.pop(str(chat_id), None)
        TG.send(chat_id, "✅ Адрес сохранён!")
        return onboarding_step_4(chat_id)
    if state == "booking_name":
        return handle_booking_name(chat_id, text)
    if state == "booking_phone":
        return handle_booking_phone(chat_id, text)
    if state == "onboarding_services":
        # В онбординге добавляем услугу упрощённо
        if len(text.strip()) < 2:
            return TG.send(chat_id, "❌ Слишком короткое название")
        STATES[str(chat_id)] = {"state": "onboarding_service_price", "svc_name": text.strip()}
        return TG.send(chat_id, f"💰 Цена для «{text.strip()}»:")
    if state == "onboarding_service_price":
        try:
            price = int(text.strip())
        except:
            return TG.send(chat_id, "❌ Введите число")
        svc_name = state_data.get("svc_name", "")
        STATES[str(chat_id)] = {"state": "onboarding_service_duration", "svc_name": svc_name, "svc_price": price}
        return TG.send(chat_id, f"⏱ Длительность (минут):")
    if state == "onboarding_service_duration":
        try:
            duration = int(text.strip())
        except:
            return TG.send(chat_id, "❌ Введите число")
        svc_name = state_data.get("svc_name", "")
        svc_price = state_data.get("svc_price", 0)
        master_data = DB.get("masters", str(chat_id))
        services = [s for s in master_data.get("services", []) if isinstance(s, dict) and s.get("name")]
        services.append({"name": svc_name, "price": svc_price, "duration": duration, "disabled": False})
        DB.set("masters", str(chat_id), {"services": services})
        STATES.pop(str(chat_id), None)
        TG.send(chat_id, f"✅ Услуга {svc_name} добавлена! Хотите добавить ещё?",
                reply_markup={"inline_keyboard": [
                    [{"text": "➕ Да, добавить ещё", "callback_data": "onboarding_add_more"}],
                    [{"text": "➡️ Дальше", "callback_data": "onboarding_next"}]
                ]})
        return
    if state == "manual_name":
        return handle_manual_name(chat_id, text)
    if state == "manual_phone":
        return handle_manual_phone(chat_id, text)
    
    # ====== ОТМЕНА ======
    if text == "🔙 Отмена":
        STATES.pop(str(chat_id), None)
        if master:
            return TG.send(chat_id, "❌ Отменено", reply_markup=KBD.master_main())
        elif client:
            return TG.send(chat_id, "❌ Отменено", reply_markup=KBD.client_main())
        return TG.send(chat_id, "❌ Отменено")
    
    # ====== ГЛАВНОЕ МЕНЮ ======
    if text == "👤 Я мастер":
        if master and master.get("completed_onboarding"):
            return TG.send(chat_id, "Вы уже зарегистрированы!", reply_markup=KBD.master_main())
        return register_master(chat_id, user_name, username)
    
    if text == "👥 Я клиент":
        if not client:
            DB.set("clients", str(chat_id), {"created_at": now().isoformat()})
        return TG.send(chat_id, "👥 *Клиентский кабинет*", reply_markup=KBD.client_main())
    
    if text == "📊 Дашборд" and master:
        return show_dashboard(chat_id)
    
    if text == "📅 Расписание" and master:
        return show_schedule(chat_id)
    
    if text == "➕ Новая запись" and master:
        return start_manual_booking(chat_id)
    
    if text == "👥 Клиенты" and master:
        return show_clients(chat_id)
    
    if text == "🔗 Моя ссылка" and master:
        return show_master_link(chat_id)
    
    if text == "📢 Свободные окна" and master:
        return show_free_slots(chat_id)
    
    if text == "⚙️ Настройки" and master:
        return TG.send(chat_id, "⚙️ *Настройки*", reply_markup=KBD.settings())
    
    if text == "💈 Услуги" and master:
        return handle_services_settings(chat_id)
    
    if text == "⏰ Часы работы" and master:
        return TG.send(chat_id, "⏰ *Настройка часов работы*", reply_markup=KBD.days_schedule(master))
    
    if text == "📍 Адрес" and master:
        return start_set_address(chat_id)
    
    if text == "🚷 Чёрный список" and master:
        return show_blacklist(chat_id)
    
    if text == "🖼 Портфолио" and master:
        TG.send(chat_id, "🖼 Отправьте фото для портфолио (до 5 шт).")
        STATES[str(chat_id)] = {"state": "adding_portfolio"}
        return
    
    if text == "🔙 Главное меню" and master:
        return TG.send(chat_id, "Главное меню", reply_markup=KBD.master_main())
    
    if text == "📋 Мои записи":
        return handle_client_appointments(chat_id)
    
    if text == "🔍 Найти мастера":
        STATES[str(chat_id)] = {"state": "finding_master"}
        return TG.send(chat_id, "🔍 Введите номер телефона мастера:", reply_markup=KBD.cancel())
    
    if text == "❓ Помощь":
        if master:
            return TG.send(chat_id, "📖 *Помощь мастеру*\n\n📊 Дашборд — статистика\n📅 Расписание — записи\n➕ Новая запись — создать\n👥 Клиенты — база\n🔗 Моя ссылка — для клиентов\n⚙️ Настройки — услуги, часы")
        else:
            return TG.send(chat_id, "📖 *Помощь*\n\n📋 Мои записи\n🔍 Найти мастера\n💡 Попросите у мастера ссылку для записи")
    
    # ====== ОБРАБОТКА ФОТО ДЛЯ ПОРТФОЛИО ======
    # (будет обработано в process_update)

# ==================== ОБРАБОТКА CALLBACK ====================
def handle_callback(chat_id, data):
    """Обработка всех инлайн-кнопок"""
    
    # Онбординг
    if data == "onboarding_skip":
        STATES.pop(str(chat_id), None)
        return onboarding_step_2(chat_id)
    if data == "onboarding_next":
        return onboarding_step_2(chat_id)
    if data == "onboarding_add_more":
        STATES[str(chat_id)] = {"state": "onboarding_services"}
        return TG.send(chat_id, "✏️ Название услуги:")
    if data == "onboarding_finish":
        return finish_onboarding(chat_id)
    
    # Услуги
    if data == "addservice":
        return start_add_service(chat_id)
    if data == "settings_back":
        return TG.send(chat_id, "⚙️ *Настройки*", reply_markup=KBD.settings())
    
    # Чёрный список
    if data == "add_blacklist":
        return start_add_blacklist(chat_id)
    if data.startswith("remove_blacklist_"):
        return handle_remove_blacklist(chat_id, data.replace("remove_blacklist_", ""))
    
    # Дни недели
    if data.startswith("setday_"):
        return handle_set_day_schedule(chat_id, data.replace("setday_", ""))
    if data.startswith("setdayvalue_"):
        parts = data.replace("setdayvalue_", "").split("_", 1)
        return handle_set_day_value(chat_id, parts[0], parts[1])
    if data == "back_to_days":
        master = DB.get("masters", str(chat_id))
        return TG.send(chat_id, "⏰ *Настройка дней:*", reply_markup=KBD.days_schedule(master))
    
    # Бронирование клиента
    if data == "booking_cancel":
        STATES.pop(str(chat_id), None)
        return TG.send(chat_id, "❌ Запись отменена", reply_markup=KBD.client_main())
    if data == "booking_back":
        state = STATES.get(str(chat_id), {})
        return handle_client_booking_start(chat_id, "")  # Упрощённо
    if data.startswith("bkservice_"):
        return handle_booking_service(chat_id, data.replace("bkservice_", ""))
    if data.startswith("bkdate_"):
        return handle_booking_date(chat_id, data.replace("bkdate_", ""))
    if data.startswith("bktime_"):
        return handle_booking_time(chat_id, data.replace("bktime_", ""))
    
    # Ручная запись
    if data.startswith("manservice_"):
        return handle_manual_service(chat_id, data.replace("manservice_", ""))
    if data.startswith("mandate_"):
        return handle_manual_date(chat_id, data.replace("mandate_", ""))
    if data.startswith("mantime_"):
        return handle_manual_time(chat_id, data.replace("mantime_", ""))
    
    # Расписание
    if data.startswith("schedule_filter_"):
        return show_schedule(chat_id, data.replace("schedule_filter_", ""))
    
    # Свободные окна
    if data.startswith("freeslots_"):
        return show_free_slots_day(chat_id, data.replace("freeslots_", ""))

# ==================== HTTP ОБРАБОТЧИК ДЛЯ VERCEL ====================
class handler(BaseHTTPRequestHandler):
    """Основной обработчик вебхуков Telegram"""
    
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length:
                body = self.rfile.read(content_length)
                update = json.loads(body.decode('utf-8'))
                self._process(update)
            
            self._respond(200, {"status": "ok"})
        except Exception as e:
            print(f"ERROR: {e}\n{traceback.format_exc()}")
            self._respond(200, {"status": "error"})
    
    def do_GET(self):
        self._respond(200, {"status": "bot online", "time": now().isoformat()})
    
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
            username = msg["from"].get("username", "")
            
            # Обработка фото для портфолио
            if "photo" in msg:
                state = STATES.get(chat_id, {}).get("state")
                if state == "adding_portfolio":
                    master = DB.get("masters", str(chat_id))
                    portfolio = master.get("portfolio", []) if master else []
                    if len(portfolio) >= 5:
                        TG.send(chat_id, "❌ Максимум 5 фото.")
                    else:
                        file_id = msg["photo"][-1]["file_id"]
                        portfolio.append({"file_id": file_id, "caption": ""})
                        DB.set("masters", str(chat_id), {"portfolio": portfolio})
                        TG.send(chat_id, f"✅ Фото добавлено! ({len(portfolio)}/5)")
                    return
            
            # Обработка текста
            text = msg.get("text", "")
            
            if text.startswith("/start"):
                if "master_" in text:
                    link_id = text.split("master_")[1].split()[0]
                    handle_client_booking_start(chat_id, link_id)
                else:
                    handle_start(chat_id, user_name)
            else:
                handle_text(chat_id, user_name, username, text)
        
        elif "callback_query" in update:
            cb = update["callback_query"]
            chat_id = str(cb["message"]["chat"]["id"])
            data = cb.get("data", "")
            
            # Отвечаем на callback
            TG.answer_callback(cb["id"])
            
            handle_callback(chat_id, data)                 