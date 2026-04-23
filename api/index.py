from http.server import BaseHTTPRequestHandler
import json
import sys
import os
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
TELEGRAM_URL = f"https://api.telegram.org/bot{TOKEN}"

def send_message(chat_id, text, reply_markup=None, parse_mode=None):
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    if parse_mode:
        payload["parse_mode"] = parse_mode
    
    response = requests.post(f"{TELEGRAM_URL}/sendMessage", json=payload)
    return response.json()

def handle_start(chat_id):
    keyboard = {
        "keyboard": [
            ["👤 Я мастер", "👥 Я клиент"]
        ],
        "resize_keyboard": True
    }
    send_message(chat_id, "Добро пожаловать в *График.Про*!\n\nКто вы?", reply_markup=keyboard, parse_mode="Markdown")

def handle_text(chat_id, text):
    if text == "👤 Я мастер":
        send_message(chat_id, "✅ Вы зарегистрированы как мастер!")
    elif text == "👥 Я клиент":
        send_message(chat_id, "✅ Вы вошли как клиент!")
    else:
        send_message(chat_id, "Используйте кнопки меню.")

def process_update(update):
    if "message" in update:
        message = update["message"]
        chat_id = message["chat"]["id"]
        
        if "text" in message:
            text = message["text"]
            if text.startswith("/start"):
                handle_start(chat_id)
            else:
                handle_text(chat_id, text)

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length else b''
        
        try:
            update = json.loads(post_data.decode('utf-8'))
            process_update(update)
        except Exception as e:
            print(f"Error: {e}")
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())
    
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "bot online"}).encode())