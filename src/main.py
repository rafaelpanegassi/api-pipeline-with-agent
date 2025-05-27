import asyncio
import time

import schedule
from loguru import logger

from core import config
from handlers.database_handler import DatabaseHandler
from handlers.telegram_handler import TelegramHandler
from processing.message_processor import process_message_data
from utils.state_manager import load_last_message_ids, save_last_message_ids


async def run_telegram_pipeline():
    logger.info("Starting Telegram message processing pipeline run...")

    if not config.API_ID or not config.API_HASH or not config.PHONE_NUMBER:
        logger.critical(
            "Telegram credentials (API_ID, API_HASH, PHONE_NUMBER) are not fully configured. Pipeline cannot run."
        )
        return
    if not config.OPENAI_API_KEY:
        logger.critical("OPENAI_API_KEY is not configured. Pipeline cannot run.")
        return

    try:
        db_handler = DatabaseHandler()
        if not await db_handler.ensure_tables_exist():
            logger.critical(
                "Could not ensure the database tables exist. Pipeline run aborted."
            )
            return
    except Exception as e:
        logger.critical(
            f"Failed to initialize DatabaseHandler or prepare database: {e}. Pipeline run aborted."
        )
        return

    last_processed_ids = load_last_message_ids()
    tg_handler = TelegramHandler()

    try:
        if not await tg_handler.connect_and_authorize():
            logger.critical(
                "Failed to connect/authorize with Telegram. Pipeline run aborted."
            )
            return

        overall_new_messages_processed_count = 0
        updated_any_id_in_state_file = False

        for chat_id_to_monitor in config.CHAT_IDS:
            chat_name = await tg_handler.get_entity_name(chat_id_to_monitor)
            min_id_for_this_chat = last_processed_ids.get(str(chat_id_to_monitor), 0)

            logger.info(
                f"Processing chat: '{chat_name}' (ID: {chat_id_to_monitor}), "
                f"fetching messages after ID: {min_id_for_this_chat}"
            )

            messages_data_for_db_batch = []
            current_max_id_found_for_this_chat = min_id_for_this_chat
            messages_fetched_this_run_for_chat = 0

            async for message_object in tg_handler.get_new_messages(
                chat_id_to_monitor, min_id_for_this_chat
            ):
                messages_fetched_this_run_for_chat += 1
                processed_data = await process_message_data(
                    message_object,
                    chat_name,
                    tg_handler.get_sender_display_name,
                )

                if processed_data:
                    messages_data_for_db_batch.append(processed_data)

                if message_object.id > current_max_id_found_for_this_chat:
                    current_max_id_found_for_this_chat = message_object.id

                batch_trigger_size = max(
                    10,
                    (
                        config.MESSAGES_FETCH_LIMIT // 5
                        if config.MESSAGES_FETCH_LIMIT > 0
                        else 10
                    ),
                )
                if len(messages_data_for_db_batch) >= batch_trigger_size:
                    logger.debug(
                        f"Sending batch of {len(messages_data_for_db_batch)} messages "
                        f"from chat '{chat_name}' to DB..."
                    )
                    count_in_batch = await db_handler.insert_messages_batch(
                        messages_data_for_db_batch
                    )
                    if count_in_batch is not None:
                        overall_new_messages_processed_count += count_in_batch
                    messages_data_for_db_batch.clear()

            if messages_data_for_db_batch:
                logger.debug(
                    f"Sending final batch of {len(messages_data_for_db_batch)} messages "
                    f"from chat '{chat_name}' to DB..."
                )
                count_in_batch = await db_handler.insert_messages_batch(
                    messages_data_for_db_batch
                )
                if count_in_batch is not None:
                    overall_new_messages_processed_count += count_in_batch
                messages_data_for_db_batch.clear()

            if messages_fetched_this_run_for_chat > 0:
                logger.info(
                    f"{messages_fetched_this_run_for_chat} messages iterated for '{chat_name}'. "
                    f"Latest ID found: {current_max_id_found_for_this_chat}"
                )
            else:
                logger.info(
                    f"No new messages found for '{chat_name}' after ID {min_id_for_this_chat}."
                )

            if current_max_id_found_for_this_chat > min_id_for_this_chat:
                last_processed_ids[str(chat_id_to_monitor)] = (
                    current_max_id_found_for_this_chat
                )
                updated_any_id_in_state_file = True

        if updated_any_id_in_state_file:
            save_last_message_ids(last_processed_ids)
            logger.info("State file (last_processed_ids.json) updated.")
        else:
            logger.info("No message IDs needed updating in the state file.")

        logger.info(
            f"Total of {overall_new_messages_processed_count} message processing attempts "
            "completed this run."
        )

    except ValueError as ve:
        logger.critical(
            f"Critical configuration or value error during pipeline run: {ve}."
        )
    except Exception as e:
        logger.exception(f"Critical unhandled error in main pipeline run: {e}")
    finally:
        logger.info("Finalizing Telegram connection for this run...")
        if tg_handler and tg_handler.client.is_connected():
            await tg_handler.disconnect()
        logger.info("Telegram message processing pipeline run finished.")


def scheduled_job_wrapper():
    """Wrapper síncrono para chamar a função async do pipeline."""
    logger.info("Scheduler starting job: Telegram Pipeline")
    try:
        asyncio.run(run_telegram_pipeline())
        logger.info("Scheduler finished job: Telegram Pipeline successfully.")
    except Exception as e:
        logger.exception("Scheduler job: Telegram Pipeline failed with an exception.")


if __name__ == "__main__":
    if hasattr(config, "setup_logging") and callable(config.setup_logging):
        config.setup_logging()
    else:
        pass

    logger.info("Application scheduler started.")

    logger.info("Running an initial job execution at startup for testing...")
    scheduled_job_wrapper()
    logger.info("Initial job execution finished.")

    logger.info(f"Telegram pipeline job scheduled to run every 12 hours.")

    schedule.every(12).hours.do(scheduled_job_wrapper)

    while True:
        schedule.run_pending()
        time.sleep(60)
