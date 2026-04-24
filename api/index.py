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

STATES = {}

def master_menu():
    return {"keyboard": [["📊 Дашборд", "📅 Расписание"], ["➕ Новая запись", "👥 Клиенты"], ["🔗 Моя ссылка", "⚙️ Настройки"]], "resize_keyboard": True}

def client_menu():
    return {"keyboard": [["📋 Мои записи"]], "resize_keyboard": True}

def settings_menu():
    return {"keyboard": [["💈 Услуги", "⏰ Часы"], ["🚫 Перерывы", "📍 Адрес"], ["🔙 В меню"]], "resize_keyboard": True}

def handle_start(chat_id, user_name):
    if firestore_get("masters", str(chat_id)):
        send_message(chat_id, f"👋 {user_name}!", reply_markup=master_menu())
    else:
        send_message(chat_id, "👋 *График.Про*\n\nКто вы?", reply_markup={"keyboard": [["👤 Я мастер"], ["👥 Я клиент"]], "resize_keyboard": True})

def handle_master_registration(chat_id, user_name, username):
    firestore_set("masters", str(chat_id), {"name": user_name, "services": [], "schedule": {"start": "09:00", "end": "18:00"}, "address": "", "completed_onboarding": False})
    send_message(chat_id, f"✅ {user_name}, добро пожаловать!")

def handle_text(chat_id, user_name, username, text):
    is_master = firestore_get("masters", str(chat_id))
    if text == "👤 Я мастер":
        handle_master_registration(chat_id, user_name, username)
    elif text == "⚙️ Настройки":
        send_message(chat_id, "⚙️ *Настройки*", reply_markup=settings_menu())
    elif text == "💈 Услуги":
        send_message(chat_id, "💈 Пока нет услуг.")
    elif text == "⏰ Часы":
        send_message(chat_id, "⏰ 09:00 – 18:00")
    elif text == "📍 Адрес":
        send_message(chat_id, "📍 Не указан")
    elif text == "🔙 В меню":
        send_message(chat_id, "Главное меню", reply_markup=master_menu() if is_master else client_menu())
    else:
        send_message(chat_id, "Используйте меню.", reply_markup=master_menu() if is_master else client_menu())

def process_update(update):
    if "message" in update:
        msg = update["message"]
        chat_id, text = msg["chat"]["id"], msg.get("text", "")
        user_name = msg["from"].get("first_name", "Пользователь")
        if text.startswith("/start"):
            handle_start(chat_id, user_name)
        else:
            handle_text(chat_id, user_name, msg["from"].get("username", ""), text)

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        cl = int(self.headers.get('Content-Length', 0))
        if cl:
            try:
                process_update(json.loads(self.rfile.read(cl).decode('utf-8')))
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