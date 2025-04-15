import os
from dotenv import load_dotenv

load_dotenv()

def get_env_list(key):
    val = os.environ.get(key, "")
    return [x.strip() for x in val.split(",") if x.strip()]

TELEGRAM_API_ID = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_PHONE = os.environ["TELEGRAM_PHONE"]
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "session")
ADMIN_IDS = [int(x) for x in get_env_list("ADMIN_IDS")]
CODE_BOT_TOKEN = os.environ["CODE_BOT_TOKEN"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
TELEGRAM_2FA_PASSWORD = os.environ.get("TELEGRAM_2FA_PASSWORD")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "parser-media") 