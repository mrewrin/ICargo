from dotenv import load_dotenv
import os

load_dotenv()

bot_token = os.getenv("BOT_TOKEN")
proxy_bot_token = os.getenv("PROXY_BOT_TOKEN")
webhook_url = os.getenv("WEBHOOK_URL")
