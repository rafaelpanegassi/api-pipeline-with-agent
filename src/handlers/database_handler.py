import asyncio
import json
from typing import Any, Dict, List, Optional

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

    async def ensure_tables_exist(self):
        """Ensures all necessary tables exist in the database."""
        success_messages_table = await self.ensure_messages_table_exists()
        if not success_messages_table:
            logger.error(
                "Table 'telegram_messages' could not be ensured. Aborting creation of other tables."
            )
            return False

        success_promotions_table = await self.ensure_promotions_data_table_exists()
        return success_messages_table and success_promotions_table

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
            extracted_urls_regex TEXT[], 
            extracted_data_raw_response JSONB, -- Armazena a resposta JSON bruta do LLM/Ollama
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (chat_id, message_id)
        );
        """
        logger.info("Verifying/Creating 'telegram_messages' table in the database...")
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

    async def ensure_promotions_data_table_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS promotions_data (
            promotion_id SERIAL PRIMARY KEY,
            message_internal_id INTEGER NOT NULL UNIQUE REFERENCES telegram_messages(internal_id) ON DELETE CASCADE,
            extraction_type TEXT, -- 'product_offer', 'coupon_only', 'irrelevant', 'error', etc.
            
            -- Campos comuns e de produto
            product_name TEXT,
            original_price NUMERIC(12, 2),
            discounted_price NUMERIC(12, 2),
            store_name TEXT,
            direct_discount_amount NUMERIC(12, 2),
            direct_discount_percentage NUMERIC(5, 2), -- Ex: 10.50 para 10.5%
            link TEXT, -- Link principal da promoção/cupom segundo o LLM

            -- Campos de cupom
            coupon_name TEXT,
            discount_description TEXT,
            coupon_discount_value_amount NUMERIC(12, 2),
            coupon_discount_value_percentage NUMERIC(5, 2),
            minimum_purchase_value_for_coupon NUMERIC(12, 2),
            minimum_purchase_value NUMERIC(12, 2),
            maximum_purchase_value NUMERIC(12, 2),
            maximum_discount_amount NUMERIC(12, 2),
            applicable_to TEXT,
            expiration_date TEXT,

            -- Campos de controle/erro
            reason TEXT, -- Para 'irrelevant' ou 'error'
            
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP -- Atualizado em cada modificação
        );
        CREATE INDEX IF NOT EXISTS idx_promotions_message_internal_id ON promotions_data(message_internal_id);
        CREATE INDEX IF NOT EXISTS idx_promotions_extraction_type ON promotions_data(extraction_type);
        CREATE INDEX IF NOT EXISTS idx_promotions_coupon_name ON promotions_data(coupon_name);
        CREATE INDEX IF NOT EXISTS idx_promotions_store_name ON promotions_data(store_name);
        CREATE INDEX IF NOT EXISTS idx_promotions_product_name ON promotions_data(product_name);
        """
        logger.info("Verifying/Creating 'promotions_data' table in the database...")
        result = await self._run_sync_db_operation(
            self.pg_manager.execute_query, create_table_query
        )
        if result is not None:
            logger.success(
                "'promotions_data' table verified/created successfully (or already existed)."
            )
            return True
        else:
            logger.error("Failed to verify/create the 'promotions_data' table.")
            return False

    async def insert_message_data_and_promotion(self, data: Dict[str, Any]) -> bool:
        """
        Inserts the main message and, if promotion data is extracted,
        inserts it into the promotions_data table.
        Returns True if the main message was inserted/updated, False otherwise.
        """
        extracted_info_dict = data.get("extracted_info")
        extracted_info_raw_str = None
        if extracted_info_dict is not None:
            try:
                extracted_info_raw_str = json.dumps(extracted_info_dict)
            except TypeError as e:
                logger.warning(
                    f"Could not serialize extracted_info to JSON: {e}. Storing as string."
                )
                extracted_info_raw_str = str(extracted_info_dict)

        insert_message_query = """
        INSERT INTO telegram_messages (
            message_id, chat_id, chat_name, sender_id, sender_name,
            message_text, message_date, media_type, extracted_urls_regex, extracted_data_raw_response
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
            message_text = EXCLUDED.message_text,
            extracted_urls_regex = EXCLUDED.extracted_urls_regex,
            extracted_data_raw_response = EXCLUDED.extracted_data_raw_response,
            processed_at = CURRENT_TIMESTAMP
        RETURNING internal_id;
        """
        message_values = (
            data.get("message_id"),
            data.get("chat_id"),
            data.get("chat_name"),
            data.get("sender_id"),
            data.get("sender_name"),
            data.get("message_text"),
            data.get("message_date"),
            data.get("media_type"),
            data.get("extracted_urls_regex"),
            extracted_info_raw_str,
        )

        result_tuples = await self._run_sync_db_operation(
            self.pg_manager.execute_query, insert_message_query, message_values
        )

        message_internal_id: Optional[int] = None
        if result_tuples and len(result_tuples) > 0 and len(result_tuples[0]) > 0:
            message_internal_id = result_tuples[0][0]
            logger.debug(
                f"Message ID {data.get('message_id')} (chat {data.get('chat_id')}) inserted/updated with internal_id: {message_internal_id}."
            )
        else:
            logger.warning(
                f"Failed to insert/update message ID {data.get('message_id')} (chat {data.get('chat_id')}) "
                f"or obtain its internal_id. Promotion data will not be saved. Result: {result_tuples}"
            )
            return False

        if (
            message_internal_id is not None
            and extracted_info_dict
            and isinstance(extracted_info_dict, dict)
        ):
            extraction_type = extracted_info_dict.get("type")

            def get_num_or_null(val, precision=None):
                if val is None or val == "":
                    return None
                try:
                    num = float(val)
                    return round(num, precision) if precision is not None else num
                except (ValueError, TypeError):
                    return None

            if extraction_type not in [
                "error",
                "no_text_content",
                "irrelevant",
                "skipped_pre_filter",
                None,
            ]:
                insert_promo_query = """
                INSERT INTO promotions_data (
                    message_internal_id, extraction_type, product_name, original_price, discounted_price,
                    store_name, direct_discount_amount, direct_discount_percentage, link,
                    coupon_name, discount_description, coupon_discount_value_amount, coupon_discount_value_percentage,
                    minimum_purchase_value_for_coupon, minimum_purchase_value, maximum_purchase_value,
                    maximum_discount_amount, applicable_to, expiration_date, reason, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                ) ON CONFLICT (message_internal_id) DO UPDATE SET
                    extraction_type = EXCLUDED.extraction_type,
                    product_name = EXCLUDED.product_name,
                    original_price = EXCLUDED.original_price,
                    discounted_price = EXCLUDED.discounted_price,
                    store_name = EXCLUDED.store_name,
                    direct_discount_amount = EXCLUDED.direct_discount_amount,
                    direct_discount_percentage = EXCLUDED.direct_discount_percentage,
                    link = EXCLUDED.link,
                    coupon_name = EXCLUDED.coupon_name,
                    discount_description = EXCLUDED.discount_description,
                    coupon_discount_value_amount = EXCLUDED.coupon_discount_value_amount,
                    coupon_discount_value_percentage = EXCLUDED.coupon_discount_value_percentage,
                    minimum_purchase_value_for_coupon = EXCLUDED.minimum_purchase_value_for_coupon,
                    minimum_purchase_value = EXCLUDED.minimum_purchase_value,
                    maximum_purchase_value = EXCLUDED.maximum_purchase_value,
                    maximum_discount_amount = EXCLUDED.maximum_discount_amount,
                    applicable_to = EXCLUDED.applicable_to,
                    expiration_date = EXCLUDED.expiration_date,
                    reason = EXCLUDED.reason,
                    updated_at = CURRENT_TIMESTAMP;
                """

                promo_values = (
                    message_internal_id,
                    extraction_type,
                    extracted_info_dict.get("product_name"),
                    get_num_or_null(extracted_info_dict.get("original_price"), 2),
                    get_num_or_null(extracted_info_dict.get("discounted_price"), 2),
                    extracted_info_dict.get("store_name"),
                    get_num_or_null(
                        extracted_info_dict.get("direct_discount_amount"), 2
                    ),
                    get_num_or_null(
                        extracted_info_dict.get("direct_discount_percentage"), 2
                    ),
                    extracted_info_dict.get("link"),
                    extracted_info_dict.get("coupon_name"),
                    extracted_info_dict.get("discount_description"),
                    get_num_or_null(
                        extracted_info_dict.get("coupon_discount_value_amount"), 2
                    ),
                    get_num_or_null(
                        extracted_info_dict.get("coupon_discount_value_percentage"), 2
                    ),
                    get_num_or_null(
                        extracted_info_dict.get("minimum_purchase_value_for_coupon"), 2
                    ),
                    get_num_or_null(
                        extracted_info_dict.get("minimum_purchase_value"), 2
                    ),
                    get_num_or_null(
                        extracted_info_dict.get("maximum_purchase_value"), 2
                    ),
                    get_num_or_null(
                        extracted_info_dict.get("maximum_discount_amount"), 2
                    ),
                    extracted_info_dict.get("applicable_to"),
                    extracted_info_dict.get("expiration_date"),
                    extracted_info_dict.get("reason"),
                )

                promo_inserted = await self._run_sync_db_operation(
                    self.pg_manager.execute_insert, insert_promo_query, promo_values
                )
                if promo_inserted:
                    logger.debug(
                        f"Promotion data for message_internal_id {message_internal_id} inserted/updated."
                    )
                else:
                    logger.warning(
                        f"Failed to insert/update promotion data for message_internal_id {message_internal_id}."
                    )
            elif extraction_type:
                insert_placeholder_promo_query = """
                 INSERT INTO promotions_data (message_internal_id, extraction_type, reason, updated_at)
                 VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                 ON CONFLICT (message_internal_id) DO UPDATE SET
                    extraction_type = EXCLUDED.extraction_type,
                    reason = EXCLUDED.reason,
                    updated_at = CURRENT_TIMESTAMP;
                 """
                placeholder_values = (
                    message_internal_id,
                    extraction_type,
                    extracted_info_dict.get("reason"),
                )
                await self._run_sync_db_operation(
                    self.pg_manager.execute_insert,
                    insert_placeholder_promo_query,
                    placeholder_values,
                )
                logger.debug(
                    f"Placeholder/Status for promotions_data (type: {extraction_type}) inserted/updated for message_internal_id {message_internal_id}."
                )

        return True

    async def insert_messages_batch(self, messages_data: List[Dict[str, Any]]) -> int:
        if not messages_data:
            return 0

        processed_successfully_count = 0
        logger.info(
            f"Starting insertion/processing of {len(messages_data)} messages in batch (iterative)."
        )

        for data_item in messages_data:
            if await self.insert_message_data_and_promotion(data_item):
                processed_successfully_count += 1

        logger.info(
            f"{processed_successfully_count}/{len(messages_data)} messages were processed "
            "(attempted insertion/update in DB, including promotion data)."
        )
        return processed_successfully_count
