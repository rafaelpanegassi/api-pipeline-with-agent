import json
from typing import Dict

from loguru import logger

from core import config


def load_last_message_ids() -> Dict[int, int]:
    try:
        with open(config.LAST_IDS_FILE, "r") as f:
            content = f.read()
            if not content:
                return {}
            return {int(k): v for k, v in json.loads(content).items()}
    except FileNotFoundError:
        logger.info(
            f"State file {config.LAST_IDS_FILE} not found. "
            "It will be created on successful first run."
        )
        return {}
    except json.JSONDecodeError:
        logger.error(
            f"Error decoding JSON from state file {config.LAST_IDS_FILE}. Returning empty map."
        )
        return {}
    except Exception as e:
        logger.error(
            f"Unexpected error loading state file {config.LAST_IDS_FILE}: {e}. Returning empty map."
        )
        return {}


def save_last_message_ids(last_ids: Dict[int, int]):
    try:
        with open(config.LAST_IDS_FILE, "w") as f:
            json.dump(last_ids, f, indent=4)
        logger.debug(
            f"Last message IDs saved successfully to {config.LAST_IDS_FILE}"
        )
    except Exception as e:
        logger.error(
            f"Failed to save last message IDs to {config.LAST_IDS_FILE}: {e}"
        )
