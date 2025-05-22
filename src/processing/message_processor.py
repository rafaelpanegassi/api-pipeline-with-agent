import re
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger
from telethon.tl.types import Channel, Chat, Message, User


def extract_urls_from_text(text: Optional[str]) -> Optional[List[str]]:
    if not text:
        return None
    url_pattern = re.compile(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    )
    urls = url_pattern.findall(text)
    return urls if urls else None


def process_message_data(
    message: Message,
    chat_name: str,
    get_sender_display_name_func: Callable[
        [Optional[Union[User, Chat, Channel]]], str
    ],
) -> Optional[Dict[str, Any]]:
    if not message or not hasattr(message, "id"):
        logger.warning(
            "process_message_data: Received an invalid message or message without an ID."
        )
        return None

    sender_name = get_sender_display_name_func(message.sender)

    media_type_str = None
    if message.media:
        media_type_str = type(message.media).__name__.replace("MessageMedia", "")

    message_text_content = message.text if hasattr(message, "text") else None
    urls_found = extract_urls_from_text(message_text_content)

    current_chat_id = message.chat_id
    if current_chat_id is None and hasattr(message.peer_id, "channel_id"):
        current_chat_id = message.peer_id.channel_id
    elif current_chat_id is None and hasattr(message.peer_id, "chat_id"):
        current_chat_id = message.peer_id.chat_id
    elif current_chat_id is None and hasattr(message.peer_id, "user_id"):
        current_chat_id = message.peer_id.user_id

    processed_data = {
        "message_id": message.id,
        "chat_id": current_chat_id,
        "chat_name": chat_name,
        "sender_id": message.sender_id,
        "sender_name": sender_name,
        "message_text": message_text_content,
        "message_date": message.date,
        "media_type": media_type_str,
        "extracted_urls": urls_found,
    }
    logger.trace(
        f"Processed message data (ID: {message.id}): "
        f"{processed_data['message_text'][:50] if processed_data['message_text'] else '[NO TEXT]'}"
    )
    return processed_data
