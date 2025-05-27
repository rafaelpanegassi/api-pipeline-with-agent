import os
import sys
from typing import List

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format=(
        "<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | "
        "<cyan>{name}:{function}:{line}</cyan> - <level>{message}</level>"
    ),
)

API_ID_STR = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")
SESSION_NAME = "telegram_messages_session"

API_ID = 0
if API_ID_STR:
    try:
        API_ID = int(API_ID_STR)
        if API_ID == 0:
            logger.warning("API_ID is configured as 0, which might be invalid.")
    except ValueError:
        logger.critical(
            f"API_ID ('{API_ID_STR}') in .env is not a valid number. Please correct it."
        )
else:
    logger.critical("API_ID is not defined in .env. Please define it.")


CHAT_IDS: List[int] = [
    -1001622757657,
    -1001686905299,
    -1001581854710,
    -1001493914605,
]
MESSAGES_FETCH_LIMIT = int(os.getenv("MESSAGES_FETCH_LIMIT", 50))
LAST_IDS_FILE = "last_processed_ids.json"

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL_NAME = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini")
OPENAI_REQUEST_TIMEOUT = int(os.getenv("OPENAI_REQUEST_TIMEOUT", 60))

def setup_logging():
    logger.info("Logging configured (default console output).")

if not API_ID:
    logger.error("Critical: Telegram API_ID is not properly configured. Script might fail.")
if not API_HASH:
    logger.error("Critical: Telegram API_HASH is not configured. Script might fail.")
if not PHONE_NUMBER:
    logger.error("Critical: Telegram PHONE_NUMBER is not configured. Script might fail.")

db_configs_present = all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT])
if not db_configs_present:
    logger.warning(
        "One or more PostgreSQL environment variables (DB_USER, DB_PASSWORD, "
        "DB_HOST, DB_NAME, DB_PORT) are not set. RDSPostgreSQLManager might fail."
    )

if not OPENAI_API_KEY:
    logger.critical("OPENAI_API_KEY is not defined in .env or config. Script will fail to process messages with OpenAI.")

logger.info(f"OpenAI configured to use model: {OPENAI_MODEL_NAME} with timeout {OPENAI_REQUEST_TIMEOUT}s")