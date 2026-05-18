import telebot
import pika
import os
import json
import time

from prometheus_client import start_http_server, Counter, Gauge

TOKEN = os.getenv('TELEGRAM_TOKEN')
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
bot = telebot.TeleBot(TOKEN)



TICKETS_SENT = Counter(
    'support_tickets_sent_total',
    'Загальна кількість надісланих заявок у чергу RabbitMQ'
)

PRODUCER_STATUS = Gauge(
    'support_producer_active_status',
    'Статус активності бота-продюсера (1 - працює, 0 - помилка)'
)


def get_rabbit_connection():
    while True:
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        except pika.exceptions.AMQPConnectionError:
            print("З'єднання з RabbitMQ... ⏳")
            time.sleep(5)


def send_ticket(user_id, username, issue):
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue='support_tickets', durable=True)

    payload = {
        "action": "NEW_TICKET",
        "user_id": user_id,
        "username": username,
        "issue": issue,
        "timestamp": time.strftime("%H:%M:%S")
    }

    channel.basic_publish(
        exchange='',
        routing_key='support_tickets',
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2)
    )


    TICKETS_SENT.inc()

    connection.close()


@bot.message_handler(commands=['ticket'])
def create_ticket(message):
    issue_text = message.text.replace('/ticket', '').strip()
    if not issue_text:
        bot.reply_to(message, "❌ Будь ласка, опишіть проблему після команди. Приклад: `/ticket Не працює логін`",
                     parse_mode="Markdown")
        return

    send_ticket(message.from_user.id, message.from_user.first_name, issue_text)
    bot.reply_to(message, "✅ Ваша заявка прийнята! Адмін скоро зв'яжеться з вами.")


@bot.message_handler(func=lambda m: True)
def info(message):
    bot.reply_to(message, "👋 Привіт! Я техпідтримка. Напиши `/ticket [твоя проблема]`, щоб створити запит.")


if __name__ == '__main__':

    try:
        start_http_server(9092)
        PRODUCER_STATUS.set(1)
        print(" 📊 Prometheus метрики продюсера доступні на порті 9092")
    except Exception as e:
        print(f" Не вдалося запустити сервер метрик: {e}")

    print("Бот-Клієнт запущений...")
    try:
        bot.polling(none_stop=True)
    except Exception:
        PRODUCER_STATUS.set(0)