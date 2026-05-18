import pika
import telebot
import os
import json
import time
import random

from prometheus_client import start_http_server, Counter, Gauge

TOKEN = os.getenv('TELEGRAM_TOKEN')
RABBIT_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
bot = telebot.TeleBot(TOKEN)

PRIORITIES = ["🟢 Низький", "🟡 Середній", "🔴 КРИТИЧНИЙ"]


TICKETS_PROCESSED = Counter(
    'support_tickets_processed_total',
    'Загальна кількість оброблених заявок консюмером'
)

TICKETS_BY_PRIORITY = Counter(
    'support_tickets_by_priority_total',
    'Кількість оброблених заявок за пріоритетами',
    ['priority']
)

CONSUMER_STATUS = Gauge(
    'support_consumer_active_status',
    'Статус активності адмін-панелі (1 - ок, 0 - сервіс лежить)'
)

def callback(ch, method, properties, body):
    ticket = json.loads(body)
    print(f" [!] Нова заявка від {ticket['username']}")

    priority = random.choice(PRIORITIES)


    admin_text = (f"🎫 **НОВА ЗАЯВКА #ST{random.randint(100, 999)}**\n"
                  f"━━━━━━━━━━━━━━━\n"
                  f"👤 **Користувач:** {ticket['username']} (ID: `{ticket['user_id']}`)\n"
                  f"📝 **Проблема:** {ticket['issue']}\n"
                  f"⏰ **Час:** {ticket['timestamp']}\n"
                  f"⚖️ **Пріоритет:** {priority}\n"
                  f"━━━━━━━━━━━━━━━\n"
                  f"⚡️ _Очікує на вашу відповідь..._")


    bot.send_message(ticket['user_id'], admin_text, parse_mode="Markdown")


    TICKETS_PROCESSED.inc()
    TICKETS_BY_PRIORITY.labels(priority=priority).inc()


    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_admin_panel():

    try:
        start_http_server(9091)
        CONSUMER_STATUS.set(1)
        print(' 📊 Prometheus метрики доступні на порті 9091')
    except Exception as e:
        print(f" Не вдалося запустити сервер метрик: {e}")

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='support_tickets', durable=True)


            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue='support_tickets', on_message_callback=callback)

            print(' 🛠️ Адмін-панель активна. Очікуємо тікети...')
            channel.start_consuming()
        except Exception:
            CONSUMER_STATUS.set(0)
            time.sleep(5)


if __name__ == '__main__':
    start_admin_panel()