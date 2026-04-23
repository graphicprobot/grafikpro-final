from http.server import BaseHTTPRequestHandler
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from telebot import TeleBot
from telebot import types as telebot_types

TOKEN = "8269135710:AAE9mv55_QJOg3VN6U7JploC6KqigKBZf6Y"
bot = TeleBot(TOKEN)

# Обработчик команды /start
@bot.message_handler(commands=["start"])
def start(message):
    markup = telebot_types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("👤 Я мастер", "👥 Я клиент")
    bot.send_message(
        message.chat.id,
        "Добро пожаловать в *График.Про*!\n\nКто вы?",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# Заглушка для кнопок
@bot.message_handler(func=lambda m: True)
def handle_text(message):
    if message.text == "👤 Я мастер":
        bot.send_message(message.chat.id, "✅ Вы зарегистрированы как мастер!")
    elif message.text == "👥 Я клиент":
        bot.send_message(message.chat.id, "✅ Вы вошли как клиент!")
    else:
        bot.send_message(message.chat.id, "Используйте кнопки меню.")

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length else b''
        
        try:
            update_dict = json.loads(post_data.decode('utf-8'))
            
            # Telegram присылает update напрямую, не в массиве
            if "update_id" in update_dict:
                bot.process_new_updates([telebot_types.Update.de_json(update_dict)])
            
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