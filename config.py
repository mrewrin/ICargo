from dotenv import load_dotenv
from fast_bitrix24 import BitrixAsync
import os

load_dotenv()

bot_token = os.getenv("BOT_TOKEN")
proxy_bot_token = os.getenv("PROXY_BOT_TOKEN")
webhook_url = os.getenv("WEBHOOK_URL")
bitrix = BitrixAsync(webhook_url)
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/clients.db")


CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/1'
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_TIMEZONE = 'UTC'