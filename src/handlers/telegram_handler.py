from typing import AsyncGenerator, Optional, Union

from loguru import logger
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import (
    ApiIdInvalidError,
    AuthKeyUnregisteredError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
)
from telethon.tl.types import Channel, Chat, Message, User

from core import config


class TelegramHandler:
    def __init__(self):
        self.api_id: int = config.API_ID
        self.api_hash: str = config.API_HASH
        self.phone_number: str = config.PHONE_NUMBER
        self.session_name: str = config.SESSION_NAME

        if not self.api_id or not self.api_hash:
            msg = (
                "Telegram credentials (API_ID, API_HASH) not correctly configured "
                "in config.py or .env."
            )
            logger.critical(msg)
            raise ValueError(msg)

        self.client = TelegramClient(
            self.session_name, self.api_id, self.api_hash
        )
        self._is_connected_and_authorized = False

    async def connect_and_authorize(self):
        if self._is_connected_and_authorized:
            logger.debug("Telegram client already connected and authorized.")
            return True
        try:
            logger.info("Connecting to Telegram...")
            await self.client.start(phone=lambda: self.phone_number)

            self._is_connected_and_authorized = True
            logger.success("Telegram client connected and authorized successfully!")
            return True

        except PhoneNumberInvalidError:
            logger.critical(
                "Provided phone number is invalid. Check config.PHONE_NUMBER."
            )
        except SessionPasswordNeededError:
            logger.critical(
                "Two-factor authentication (2FA) is enabled and password is required. "
                "The script does not handle this interactively after the first time. "
                "Authenticate manually or provide the password."
            )
        except ApiIdInvalidError:
            logger.critical(
                "Invalid API_ID or API_HASH. Check your credentials on my.telegram.org."
            )
        except (UserDeactivatedBanError, AuthKeyUnregisteredError):
            logger.critical(
                "Telegram account deactivated, banned, or auth key invalid. "
                "May need to recreate session or check account."
            )
        except ConnectionError as e:
            logger.critical(
                f"Connection failure with Telegram: Network issue or Telegram servers. {e}"
            )
        except Exception as e:
            logger.critical(
                f"Unexpected error while connecting and authorizing with Telegram: {e}"
            )

        self._is_connected_and_authorized = False
        return False

    async def disconnect(self):
        if self.client.is_connected():
            await self.client.disconnect()
            self._is_connected_and_authorized = False
            logger.info("Telegram client disconnected.")

    async def get_entity_name(self, entity_id: int) -> str:
        if not self._is_connected_and_authorized:
            logger.warning(
                "Attempting to get entity name without connection/authorization."
            )
            if not await self.connect_and_authorize():
                return f"ID: {entity_id} (Connection failed)"

        try:
            entity: Union[User, Chat, Channel] = await self.client.get_entity(
                entity_id
            )
            if hasattr(entity, "title") and entity.title:
                return entity.title
            if hasattr(entity, "username") and entity.username:
                return f"@{entity.username}"
            if hasattr(entity, "first_name") and entity.first_name:
                name = entity.first_name
                if hasattr(entity, "last_name") and entity.last_name:
                    name += f" {entity.last_name}"
                return name
            return f"ID: {entity_id} (Unknown Type)"
        except ValueError:
            logger.error(
                f"Invalid or not found entity ID in Telegram: {entity_id}"
            )
            return f"ID: {entity_id} (Invalid/Not Found)"
        except Exception as e:
            logger.error(
                f"Error getting entity name {entity_id} from Telegram: {e}"
            )
            return f"ID: {entity_id} (Error fetching name)"

    def get_sender_display_name(
        self, sender: Optional[Union[User, Chat, Channel]]
    ) -> str:
        if not sender:
            return "Unknown"
        if hasattr(sender, "title") and sender.title:
            return sender.title
        if hasattr(sender, "username") and sender.username:
            return f"@{sender.username}"
        if hasattr(sender, "first_name") and sender.first_name:
            name = sender.first_name
            if hasattr(sender, "last_name") and sender.last_name:
                name += f" {sender.last_name}"
            return name
        if hasattr(sender, "id"):
            return f"SenderID: {sender.id}"
        return "Unknown"

    async def get_new_messages(
        self, chat_id: int, min_id: int
    ) -> AsyncGenerator[Message, None]:
        if not self._is_connected_and_authorized:
            logger.warning(
                f"Attempting to fetch messages for chat {chat_id} without connection/authorization."
            )
            if not await self.connect_and_authorize():
                logger.error(
                    f"Could not connect to fetch messages from chat {chat_id}."
                )
                return

        logger.debug(
            f"Fetching messages for chat {chat_id} with min_id {min_id}. Limit: {config.MESSAGES_FETCH_LIMIT}"
        )
        try:
            messages_batch = []
            async for message in self.client.iter_messages(
                chat_id, limit=config.MESSAGES_FETCH_LIMIT, min_id=min_id
            ):
                messages_batch.append(message)

            for message in reversed(messages_batch):
                yield message

        except Exception as e:
            logger.error(
                f"Error fetching Telegram messages for chat {chat_id} (min_id: {min_id}): {e}"
            )
