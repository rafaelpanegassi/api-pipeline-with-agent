import asyncio
import os
import sys
import json # Para salvar e carregar os IDs das últimas mensagens
from typing import List, Optional, Union, Dict

from dotenv import load_dotenv
from loguru import logger
from telethon.sync import TelegramClient
# GetHistoryRequest é bom, mas client.iter_messages pode ser mais conveniente para min_id
# from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Channel, Chat, User, Message
from telethon.errors.rpcerrorlist import PhoneNumberInvalidError, SessionPasswordNeededError, ApiIdInvalidError

# --- 1. Carregar variáveis de ambiente do arquivo .env ---
load_dotenv()

# --- 2. Constantes de Configuração ---
API_ID_STR = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH', '')
PHONE_NUMBER = os.getenv('PHONE_NUMBER', '')
SESSION_NAME = 'telegram_messages_session'
LAST_IDS_FILE = 'last_processed_ids.json' # Arquivo para guardar os IDs

CHAT_IDS: List[int] = [
    -1001622757657,
    -1001686905299,
    -1001581854710,
    -1001493914605
]

LOG_FILE_PATH = "file_telegram_messages.log"
LOG_ROTATION = "10 MB"
LOG_RETENTION = "7 days"
MESSAGES_FETCH_LIMIT = 50 # Limite de quantas novas mensagens buscar por vez (ajuste conforme necessário)

# --- 3. Configuração do Loguru ---
def setup_logging():
    logger.remove()
    logger.add(
        LOG_FILE_PATH,
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}"
    )
    logger.add(
        sys.stderr,
        level="INFO",
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> <level>{level: <8}</level> <bold>{message}</bold>"
    )
    logger.info("Logging configurado.")

# --- 4. Funções de Persistência de IDs ---
def load_last_message_ids() -> Dict[int, int]:
    """Carrega os IDs das últimas mensagens processadas do arquivo JSON."""
    try:
        with open(LAST_IDS_FILE, 'r') as f:
            content = f.read()
            if not content: # Arquivo vazio
                return {}
            # Converte chaves de string para int após carregar do JSON
            return {int(k): v for k, v in json.loads(content).items()}
    except FileNotFoundError:
        logger.info(f"Arquivo {LAST_IDS_FILE} não encontrado. Será criado um novo.")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Erro ao decodificar JSON do arquivo {LAST_IDS_FILE}. Retornando mapa vazio.")
        return {}
    except Exception as e:
        logger.error(f"Erro ao carregar {LAST_IDS_FILE}: {e}. Retornando mapa vazio.")
        return {}

def save_last_message_ids(last_ids: Dict[int, int]):
    """Salva os IDs das últimas mensagens processadas no arquivo JSON."""
    try:
        with open(LAST_IDS_FILE, 'w') as f:
            json.dump(last_ids, f, indent=4)
        logger.debug(f"IDs das últimas mensagens salvos em {LAST_IDS_FILE}")
    except Exception as e:
        logger.error(f"Erro ao salvar {LAST_IDS_FILE}: {e}")

# --- 5. Funções Auxiliares do Telegram ---
async def get_entity_name(client: TelegramClient, entity_id: int) -> str:
    try:
        entity: Union[User, Chat, Channel] = await client.get_entity(entity_id)
        if hasattr(entity, 'title') and entity.title: return entity.title
        if hasattr(entity, 'username') and entity.username: return f"@{entity.username}"
        if hasattr(entity, 'first_name') and entity.first_name:
            name = entity.first_name
            if hasattr(entity, 'last_name') and entity.last_name: name += f" {entity.last_name}"
            return name
        return f"ID: {entity_id} (Tipo Desconhecido)"
    except ValueError:
        logger.error(f"ID de entidade inválido: {entity_id}")
        return f"ID: {entity_id} (Inválido)"
    except Exception as e:
        logger.error(f"Erro ao obter entidade para ID {entity_id}: {e}")
        return f"ID: {entity_id} (Nome Desconhecido)"

def get_sender_display_name(sender: Optional[Union[User, Chat, Channel]]) -> str:
    if not sender: return "Desconhecido"
    if hasattr(sender, 'title') and sender.title: return sender.title
    if hasattr(sender, 'username') and sender.username: return f"@{sender.username}"
    if hasattr(sender, 'first_name') and sender.first_name:
        name = sender.first_name
        if hasattr(sender, 'last_name') and sender.last_name: name += f" {sender.last_name}"
        return name
    if hasattr(sender, 'id'): return f"SenderID: {sender.id}"
    return "Desconhecido"

def log_message_details(message: Message, chat_name: str):
    timestamp = message.date.strftime('%Y-%m-%d %H:%M:%S')
    sender_display_name = get_sender_display_name(message.sender)

    if message.text:
        logger.info(f"[{chat_name}] [{timestamp}] {sender_display_name}: {message.text}")
    elif message.media:
        media_type = type(message.media).__name__.replace('MessageMedia', '')
        logger.info(f"[{chat_name}] [{timestamp}] {sender_display_name}: [MÍDIA: {media_type}]")
    else:
        logger.debug(f"[{chat_name}] [{timestamp}] {sender_display_name}: [Msg tipo desconhecido/vazia, ID: {message.id}]")


async def fetch_and_log_new_messages_for_chat(
    client: TelegramClient,
    chat_id: int,
    last_message_ids: Dict[int, int]
) -> Optional[int]:
    """
    Busca e loga mensagens novas para um chat específico.
    Retorna o ID da mensagem mais recente encontrada, ou None se nenhuma nova.
    """
    chat_name = await get_entity_name(client, chat_id)
    last_known_id = last_message_ids.get(chat_id, 0)

    logger.info(f"\n--- Buscando NOVAS mensagens para: {chat_name} (ID: {chat_id}) após msg ID: {last_known_id} ---")

    new_messages_found = []
    current_max_id_for_chat = last_known_id

    try:
        # Usamos client.iter_messages com min_id para pegar mensagens MAIS NOVAS
        # que last_known_id. As mensagens vêm da mais nova para a mais antiga.
        async for message in client.iter_messages(
            chat_id,
            limit=MESSAGES_FETCH_LIMIT, # Limita quantas mensagens novas processar por vez
            min_id=last_known_id # Só mensagens com ID > last_known_id
        ):
            new_messages_found.append(message)
            if message.id > current_max_id_for_chat:
                current_max_id_for_chat = message.id

        if not new_messages_found:
            logger.info(f"Nenhuma mensagem NOVA encontrada para {chat_name} (ID: {chat_id}) após msg ID: {last_known_id}.")
            return None # Nenhuma mensagem nova, não atualiza o ID

        # As mensagens vêm da mais nova para a mais antiga por padrão com iter_messages.
        # Se quisermos logar na ordem cronológica (mais antiga primeiro da leva):
        for message in reversed(new_messages_found):
            log_message_details(message, chat_name)
        
        logger.success(f"{len(new_messages_found)} nova(s) mensagem(ns) processada(s) para {chat_name}. Último ID agora: {current_max_id_for_chat}")
        return current_max_id_for_chat # Retorna o ID da mensagem mais recente desta leva

    except Exception as e:
        logger.error(f"Erro ao buscar NOVAS mensagens de {chat_name} (ID: {chat_id}): {e}")
        return None


# --- 6. Função Principal ---
async def main():
    setup_logging()
    logger.info("Iniciando script de busca de NOVAS mensagens do Telegram...")

    if not API_ID_STR:
        logger.critical("API_ID não encontrado.")
        return
    try:
        api_id = int(API_ID_STR)
    except ValueError:
        logger.critical(f"API_ID ('{API_ID_STR}') inválido.")
        return
    if not API_HASH: logger.critical("API_HASH não encontrado."); return
    if not PHONE_NUMBER: logger.critical("PHONE_NUMBER não encontrado."); return

    last_message_ids = load_last_message_ids()

    async with TelegramClient(SESSION_NAME, api_id, API_HASH) as client:
        try:
            await client.start(phone=lambda: PHONE_NUMBER)
            logger.success("Cliente Telegram conectado!")
        except (PhoneNumberInvalidError, SessionPasswordNeededError, ApiIdInvalidError, ConnectionError) as e:
            logger.critical(f"Falha crítica ao conectar ao Telegram: {e}")
            return
        except Exception as e:
            logger.critical(f"Falha ao conectar ao Telegram: {e}")
            return

        if not await client.is_user_authorized():
            logger.warning("Usuário não autorizado. Insira o código no console.")
            # Telethon pedirá o código se necessário

        updated_any_id = False
        for chat_id_item in CHAT_IDS:
            new_max_id = await fetch_and_log_new_messages_for_chat(client, chat_id_item, last_message_ids)
            if new_max_id is not None and new_max_id > last_message_ids.get(chat_id_item, 0) :
                last_message_ids[chat_id_item] = new_max_id
                updated_any_id = True
        
        if updated_any_id:
            save_last_message_ids(last_message_ids)
        else:
            logger.info("Nenhum ID de mensagem precisou ser atualizado.")

    logger.info("Script de busca de NOVAS mensagens do Telegram finalizado.")

# --- 7. Executa a função principal ---
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro inesperado na execução do script: {e}", exc_info=True)