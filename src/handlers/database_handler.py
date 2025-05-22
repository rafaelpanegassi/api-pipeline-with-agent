import asyncio
from typing import Any, Dict, List

from loguru import logger

from core import config
from tools.rds_postgres_manager import RDSPostgreSQLManager


class DatabaseHandler:
    def __init__(self):
        try:
            self.pg_manager = RDSPostgreSQLManager(
                db_name=config.DB_NAME,
                db_user=config.DB_USER,
                db_password=config.DB_PASSWORD,
                db_host=config.DB_HOST,
                db_port=config.DB_PORT,
            )
            logger.info("DatabaseHandler initialized with RDSPostgreSQLManager.")
        except ValueError as e:
            logger.critical(
                f"Failed to initialize RDSPostgreSQLManager: {e}. "
                "Check database configurations in .env."
            )
            raise

    async def _run_sync_db_operation(self, func, *args):
        try:
            return await asyncio.to_thread(func, *args)
        except Exception as e:
            logger.error(
                f"Error during synchronous DB operation ({func.__name__}): {e}"
            )
            return None

    async def ensure_messages_table_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS telegram_messages (
            internal_id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            chat_name TEXT,
            sender_id BIGINT,
            sender_name TEXT,
            message_text TEXT,
            message_date TIMESTAMP WITH TIME ZONE NOT NULL,
            media_type TEXT,
            extracted_urls TEXT[],
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (chat_id, message_id)
        );
        """
        logger.info(
            "Verifying/Creating 'telegram_messages' table in the database..."
        )
        result = await self._run_sync_db_operation(
            self.pg_manager.execute_query, create_table_query
        )

        if result is not None:
            logger.success(
                "'telegram_messages' table verified/created successfully (or already existed)."
            )
            return True
        else:
            logger.error("Failed to verify/create the 'telegram_messages' table.")
            return False

    async def insert_message_data(self, data: Dict[str, Any]) -> bool:
        insert_query = """
        INSERT INTO telegram_messages (
            message_id, chat_id, chat_name, sender_id, sender_name,
            message_text, message_date, media_type, extracted_urls
        ) VALUES (
            %(message_id)s, %(chat_id)s, %(chat_name)s, %(sender_id)s, %(sender_name)s,
            %(message_text)s, %(message_date)s, %(media_type)s, %(extracted_urls)s
        ) ON CONFLICT (chat_id, message_id) DO NOTHING;
        """
        values_tuple = (
            data.get("message_id"),
            data.get("chat_id"),
            data.get("chat_name"),
            data.get("sender_id"),
            data.get("sender_name"),
            data.get("message_text"),
            data.get("message_date"),
            data.get("media_type"),
            data.get("extracted_urls"),
        )

        success = await self._run_sync_db_operation(
            self.pg_manager.execute_insert, insert_query, values_tuple
        )

        if success:
            return True
        else:
            logger.warning(
                f"Failed to insert message ID {data.get('message_id')} "
                f"(chat {data.get('chat_id')}) into the database."
            )
            return False

    async def insert_messages_batch(
        self, messages_data: List[Dict[str, Any]]
    ) -> int:
        if not messages_data:
            return 0

        inserted_count = 0
        logger.info(
            f"Starting insertion of {len(messages_data)} messages in 'batch' (iterative)."
        )

        for data in messages_data:
            if await self.insert_message_data(data):
                inserted_count += 1

        logger.info(
            f"{inserted_count}/{len(messages_data)} message insertion attempts were successful "
            "(may include ignored conflicts)."
        )
        return inserted_count
import asyncio
from typing import Any, Dict, List

from loguru import logger

from core import config
from tools.rds_postgres_manager import RDSPostgreSQLManager


class DatabaseHandler:
    def __init__(self):
        try:
            self.pg_manager = RDSPostgreSQLManager(
                db_name=config.DB_NAME,
                db_user=config.DB_USER,
                db_password=config.DB_PASSWORD,
                db_host=config.DB_HOST,
                db_port=config.DB_PORT,
            )
            logger.info("DatabaseHandler initialized with RDSPostgreSQLManager.")
        except ValueError as e:
            logger.critical(
                f"Failed to initialize RDSPostgreSQLManager: {e}. "
                "Check database configurations in .env."
            )
            raise

    async def _run_sync_db_operation(self, func, *args):
        try:
            return await asyncio.to_thread(func, *args)
        except Exception as e:
            logger.error(
                f"Error during synchronous DB operation ({func.__name__}): {e}"
            )
            return None

    async def ensure_messages_table_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS telegram_messages (
            internal_id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            chat_name TEXT,
            sender_id BIGINT,
            sender_name TEXT,
            message_text TEXT,
            message_date TIMESTAMP WITH TIME ZONE NOT NULL,
            media_type TEXT,
            extracted_urls TEXT[],
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (chat_id, message_id)
        );
        """
        logger.info(
            "Verifying/Creating 'telegram_messages' table in the database..."
        )
        result = await self._run_sync_db_operation(
            self.pg_manager.execute_query, create_table_query
        )

        if result is not None:
            logger.success(
                "'telegram_messages' table verified/created successfully (or already existed)."
            )
            return True
        else:
            logger.error("Failed to verify/create the 'telegram_messages' table.")
            return False

    async def insert_message_data(self, data: Dict[str, Any]) -> bool:
        insert_query = """
        INSERT INTO telegram_messages (
            message_id, chat_id, chat_name, sender_id, sender_name,
            message_text, message_date, media_type, extracted_urls
        ) VALUES (
            %(message_id)s, %(chat_id)s, %(chat_name)s, %(sender_id)s, %(sender_name)s,
            %(message_text)s, %(message_date)s, %(media_type)s, %(extracted_urls)s
        ) ON CONFLICT (chat_id, message_id) DO NOTHING;
        """
        values_tuple = (
            data.get("message_id"),
            data.get("chat_id"),
            data.get("chat_name"),
            data.get("sender_id"),
            data.get("sender_name"),
            data.get("message_text"),
            data.get("message_date"),
            data.get("media_type"),
            data.get("extracted_urls"),
        )

        success = await self._run_sync_db_operation(
            self.pg_manager.execute_insert, insert_query, values_tuple
        )

        if success:
            return True
        else:
            logger.warning(
                f"Failed to insert message ID {data.get('message_id')} "
                f"(chat {data.get('chat_id')}) into the database."
            )
            return False

    async def insert_messages_batch(
        self, messages_data: List[Dict[str, Any]]
    ) -> int:
        if not messages_data:
            return 0

        inserted_count = 0
        logger.info(
            f"Starting insertion of {len(messages_data)} messages in 'batch' (iterative)."
        )

        for data in messages_data:
            if await self.insert_message_data(data):
                inserted_count += 1

        logger.info(
            f"{inserted_count}/{len(messages_data)} message insertion attempts were successful "
            "(may include ignored conflicts)."
        )
        return inserted_count
