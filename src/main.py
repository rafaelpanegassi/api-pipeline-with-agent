import asyncio

from loguru import logger

from core import config
from handlers.database_handler import DatabaseHandler
from handlers.telegram_handler import TelegramHandler
from processing.message_processor import process_message_data # process_message_data agora é async
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
        # Garante que AMBAS as tabelas existam
        if not await db_handler.ensure_tables_exist():
            logger.critical(
                "Could not ensure the database tables exist. Exiting."
            )
            return
    except Exception as e:
        logger.critical(
            f"Failed to initialize DatabaseHandler or prepare database: {e}. Exiting."
        )
        return

    last_processed_ids = load_last_message_ids() # Retorna Dict[str, int]
    tg_handler = TelegramHandler()

    try:
        if not await tg_handler.connect_and_authorize():
            logger.critical("Failed to connect/authorize with Telegram. Exiting.")
            return

        overall_new_messages_processed_count = 0
        updated_any_id_in_state_file = False

        for chat_id_to_monitor in config.CHAT_IDS: # chat_id_to_monitor é int
            chat_name = await tg_handler.get_entity_name(chat_id_to_monitor)
            # Usa str(chat_id) para consistência com chaves JSON no state_manager
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
                # process_message_data agora é async e precisa de await
                processed_data = await process_message_data(
                    message_object,
                    chat_name,
                    tg_handler.get_sender_display_name,
                )

                if processed_data:
                    messages_data_for_db_batch.append(processed_data)

                if message_object.id > current_max_id_found_for_this_chat:
                    current_max_id_found_for_this_chat = message_object.id

                # Lógica de batch para inserção no DB
                if len(messages_data_for_db_batch) >= config.MESSAGES_FETCH_LIMIT / 5 or len(messages_data_for_db_batch) >=10 : # Ajuste o tamanho do batch
                    logger.debug(
                        f"Sending batch of {len(messages_data_for_db_batch)} messages "
                        f"from chat '{chat_name}' to DB..."
                    )
                    # A função insert_messages_batch agora lida com a mensagem e a promoção
                    count_in_batch = await db_handler.insert_messages_batch(messages_data_for_db_batch)
                    overall_new_messages_processed_count += count_in_batch
                    messages_data_for_db_batch.clear()
            
            # Enviar batch final, se houver
            if messages_data_for_db_batch:
                logger.debug(
                    f"Sending final batch of {len(messages_data_for_db_batch)} messages "
                    f"from chat '{chat_name}' to DB..."
                )
                count_in_batch = await db_handler.insert_messages_batch(messages_data_for_db_batch)
                overall_new_messages_processed_count += count_in_batch
                messages_data_for_db_batch.clear() # Boa prática

            if messages_fetched_this_run_for_chat > 0:
                logger.info(
                    f"{messages_fetched_this_run_for_chat} messages iterated for '{chat_name}'. "
                    f"Latest ID found: {current_max_id_found_for_this_chat}"
                )
            else:
                logger.info(f"No new messages found for '{chat_name}' after ID {min_id_for_this_chat}.")

            if current_max_id_found_for_this_chat > min_id_for_this_chat:
                # Usa str(chat_id) para consistência com chaves JSON no state_manager
                last_processed_ids[str(chat_id_to_monitor)] = current_max_id_found_for_this_chat
                updated_any_id_in_state_file = True
        
        if updated_any_id_in_state_file:
            save_last_message_ids(last_processed_ids) # Espera Dict[str, int]
            logger.info("State file (last_processed_ids.json) updated.")
        else:
            logger.info("No message IDs needed updating in the state file.")

        logger.info(
            f"Total of {overall_new_messages_processed_count} message processing attempts "
            "completed this run."
        )

    except ValueError as ve: # Captura ValueErrors que podem vir da inicialização
        logger.critical(f"Critical configuration or value error: {ve}. Exiting.")
    except Exception as e:
        logger.opt(exception=True).critical(f"Critical unhandled error in main pipeline: {e}")
    finally:
        logger.info("Finalizing connections...")
        if 'tg_handler' in locals() and tg_handler and tg_handler.client.is_connected():
            await tg_handler.disconnect()
        # RDSPostgreSQLManager não parece ter um método de desconexão explícito no seu uso atual,
        # pois as conexões são abertas e fechadas por operação.
        logger.info("Telegram message processing pipeline finished.")


if __name__ == "__main__":
    try:
        asyncio.run(run_telegram_pipeline())
    except KeyboardInterrupt:
        logger.warning("Script interrupted by user (KeyboardInterrupt).")
    except Exception as e: # Captura erros que podem ocorrer fora do run_telegram_pipeline mas dentro do asyncio.run
        logger.opt(exception=True).critical(f"Fatal unhandled error during asyncio.run: {e}")

