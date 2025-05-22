import asyncio

from loguru import logger

from core import config
from handlers.database_handler import DatabaseHandler
from handlers.telegram_handler import TelegramHandler
from processing.message_processor import process_message_data
from utils.state_manager import load_last_message_ids, save_last_message_ids


async def run_telegram_pipeline():
    logger.info("Starting Telegram message processing pipeline...")

    if not config.API_ID or not config.API_HASH or not config.PHONE_NUMBER:
        logger.critical(
            "Telegram credentials (API_ID, API_HASH, PHONE_NUMBER) are not fully configured. Exiting."
        )
        return

    try:
        db_handler = DatabaseHandler()
        if not await db_handler.ensure_messages_table_exists():
            logger.critical(
                "Could not ensure the messages table exists in the database. Exiting."
            )
            return
    except Exception as e:
        logger.critical(
            f"Failed to initialize DatabaseHandler or prepare database: {e}. Exiting."
        )
        return

    last_processed_ids = load_last_message_ids()
    tg_handler = TelegramHandler()

    try:
        if not await tg_handler.connect_and_authorize():
            logger.critical("Failed to connect/authorize with Telegram. Exiting.")
            return

        overall_new_messages_processed_count = 0
        updated_any_id_in_state_file = False

        for chat_id_to_monitor in config.CHAT_IDS:
            chat_name = await tg_handler.get_entity_name(chat_id_to_monitor)
            min_id_for_this_chat = last_processed_ids.get(chat_id_to_monitor, 0)

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
                processed_data = process_message_data(
                    message_object,
                    chat_name,
                    tg_handler.get_sender_display_name,
                )

                if processed_data:
                    messages_data_for_db_batch.append(processed_data)

                if message_object.id > current_max_id_found_for_this_chat:
                    current_max_id_found_for_this_chat = message_object.id

                if len(messages_data_for_db_batch) >= 10:
                    logger.debug(
                        f"Sending batch of {len(messages_data_for_db_batch)} messages "
                        f"from chat '{chat_name}' to DB..."
                    )
                    await db_handler.insert_messages_batch(
                        messages_data_for_db_batch
                    )
                    overall_new_messages_processed_count += len(
                        messages_data_for_db_batch
                    )
                    messages_data_for_db_batch.clear()

            if messages_data_for_db_batch:
                logger.debug(
                    f"Sending final batch of {len(messages_data_for_db_batch)} messages "
                    f"from chat '{chat_name}' to DB..."
                )
                await db_handler.insert_messages_batch(
                    messages_data_for_db_batch
                )
                overall_new_messages_processed_count += len(
                    messages_data_for_db_batch
                )

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
                last_processed_ids[
                    chat_id_to_monitor
                ] = current_max_id_found_for_this_chat
                updated_any_id_in_state_file = True

        if updated_any_id_in_state_file:
            save_last_message_ids(last_processed_ids)
            logger.info("State file (last_processed_ids.json) updated.")
        else:
            logger.info(
                "No message IDs needed updating in the state file."
            )

        logger.info(
            f"Total of {overall_new_messages_processed_count} messages processed and "
            "sent to the database this run."
        )

    except ValueError as ve:
        logger.critical(f"Critical configuration error: {ve}. Exiting.")
    except Exception as e:
        logger.opt(exception=True).critical(
            f"Critical unhandled error in main pipeline: {e}"
        )
    finally:
        logger.info("Finalizing connections...")
        if tg_handler:
            await tg_handler.disconnect()
        logger.info("Telegram message processing pipeline finished.")


if __name__ == "__main__":
    try:
        asyncio.run(run_telegram_pipeline())
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user (KeyboardInterrupt).")
    except Exception as e:
        logger.opt(exception=True).critical(
            f"Fatal unhandled error during asyncio.run: {e}"
        )
