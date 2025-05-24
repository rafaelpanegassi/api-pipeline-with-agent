import json
import os
from typing import Dict
from loguru import logger
from core import config # Para LAST_IDS_FILE

def load_last_message_ids() -> Dict[str, int]: # Chave é string (chat_id como string)
    """Loads the last processed message IDs from a JSON file."""
    if os.path.exists(config.LAST_IDS_FILE):
        try:
            with open(config.LAST_IDS_FILE, "r") as f:
                ids = json.load(f)
                # Ensure keys are strings and values are integers
                return {str(k): int(v) for k, v in ids.items()}
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Error loading or parsing '{config.LAST_IDS_FILE}': {e}. Using zeroed IDs.")
            return {} # Return empty dict or with defaults if file is corrupted
    logger.info(f"File '{config.LAST_IDS_FILE}' not found. Starting without prior IDs.")
    return {}

def save_last_message_ids(last_ids: Dict[str, int]): # Expects Dict with string keys
    """Saves the last processed message IDs to a JSON file."""
    try:
        # Ensure all keys are strings before saving, just in case
        string_keyed_ids = {str(k): int(v) for k, v in last_ids.items()}
        with open(config.LAST_IDS_FILE, "w") as f:
            json.dump(string_keyed_ids, f, indent=4)
        logger.info(f"Last message IDs saved to '{config.LAST_IDS_FILE}'.")
    except IOError as e:
        logger.error(f"Error saving last message IDs to '{config.LAST_IDS_FILE}': {e}")
    except Exception as e:
        logger.error(f"Unexpected error saving last message IDs: {e}")

