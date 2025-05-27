import asyncio
import json
import re
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger
from openai import AsyncOpenAI, OpenAIError
from telethon.tl.types import Channel, Chat, Message, User

from core import config

LLM_PROMOTION_EXTRACTION_PROMPT_TEMPLATE = """
Analise a seguinte mensagem do Telegram e extraia informações sobre promoções, produtos ou cupons.
Responda APENAS com um objeto JSON. Não inclua NENHUM texto explicativo, introdução, ou qualquer coisa fora do objeto JSON.
O JSON deve seguir estritamente esta estrutura e tipos de dados.

Se a mensagem for sobre um PRODUTO específico em promoção:
Retorne um JSON com "type": "product_offer" e os seguintes campos (use null se não encontrar ou não aplicável):
- "product_name": (string) Nome do produto.
- "original_price": (float) Preço original do produto.
- "discounted_price": (float) Preço do produto com desconto.
- "store_name": (string, opcional) Nome da loja.
- "coupon_name": (string, opcional) Código do cupom, se aplicável à oferta do produto.
- "coupon_discount_value_amount": (float, opcional) Valor do desconto do cupom em reais.
- "coupon_discount_value_percentage": (float, opcional) Valor do desconto do cupom em porcentagem (ex: 10 para 10%).
- "minimum_purchase_value_for_coupon": (float, opcional) Compra mínima para o cupom, se ligado ao produto.
- "direct_discount_amount": (float, opcional) Desconto direto em reais (ex: "economize R$50").
- "direct_discount_percentage": (float, opcional) Desconto direto em porcentagem (ex: "25% OFF").
- "link": (string, opcional) Principal link da promoção do produto.

Se a mensagem for APENAS sobre um CUPOM de desconto (sem um produto específico):
Retorne um JSON com "type": "coupon_only" e os seguintes campos (use null se não encontrar ou não aplicável):
- "coupon_name": (string) Código do cupom.
- "discount_description": (string) Descrição do que o cupom oferece.
- "store_name": (string, opcional) Nome da loja onde o cupom é válido.
- "coupon_discount_value_amount": (float, opcional) Valor do desconto em reais (ex: "R$20 de desconto").
- "coupon_discount_value_percentage": (float, opcional) Valor do desconto em porcentagem (ex: "15% OFF").
- "minimum_purchase_value": (float, opcional) Compra mínima para usar o cupom.
- "maximum_purchase_value": (float, opcional) Compra máxima para usar o cupom.
- "maximum_discount_amount": (float, opcional) Desconto máximo que o cupom pode fornecer em reais.
- "applicable_to": (string, opcional) Onde o cupom se aplica (ex: "todo o site", "categoria X", "produtos selecionados", "primeira compra").
- "expiration_date": (string, opcional) Data de validade do cupom (formato AAAA-MM-DD se possível, ou texto original).
- "link": (string, opcional) Link para usar o cupom ou ver as regras.

Se a mensagem NÃO contiver informações claras sobre promoções, produtos com desconto ou cupons:
Retorne um JSON com "type": "irrelevant", "reason": "A mensagem não parece ser uma promoção ou cupom."

Instruções Adicionais para o JSON:
- Use `null` para campos não encontrados ou não aplicáveis. NÃO omita campos da estrutura base.
- Converta todos os valores monetários para números (float), removendo "R$", vírgulas de milhar e usando ponto como separador decimal.
- Se um produto tem preço "de X por Y", X é original_price e Y é discounted_price.
- Se um cupom diz "X% até R$Y", X é coupon_discount_value_percentage e Y é maximum_discount_amount.
- O campo "link" deve ser o link MAIS RELEVANTE para a oferta ou cupom.
- GARANTA QUE A SAÍDA SEJA APENAS O JSON, SEM TEXTO ADICIONAL.

Mensagem para análise:
\"\"\"
{message_text}
\"\"\"

Objeto JSON extraído:
"""

RELEVANT_KEYWORDS_FOR_PRE_FILTER = [
    "r$",
    "promo",
    "desconto",
    "cupom",
    "oferta",
    "%",
    " off",
    "barato",
    "preço",
    "imperdível",
    "saldão",
    "liquida",
    "frete grátis",
    "grátis",
    "compre",
    "ganhe",
    "economize",
    "leve",
    "pague",
    "cashback",
    "metade do preço",
]

if config.OPENAI_API_KEY:
    openai_async_client = AsyncOpenAI(
        api_key=config.OPENAI_API_KEY, timeout=config.OPENAI_REQUEST_TIMEOUT
    )
else:
    openai_async_client = None
    logger.warning("OpenAI API Key not found. OpenAI client not initialized.")


def is_potentially_promotional(text: Optional[str]) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    if any(keyword in text_lower for keyword in RELEVANT_KEYWORDS_FOR_PRE_FILTER):
        return True
    if (
        re.search(r"\d+([,\.]\d{2})?\s*%", text_lower)
        or re.search(r"R\$\s*\d+([,\.]\d{2})?", text_lower)
        or re.search(r"de\s+R\$\s*[\d,.]+\s+por\s+R\$\s*[\d,.]+", text_lower)
    ):
        return True
    return False


async def extract_promotion_info_with_openai(
    message_text: str,
) -> Optional[Dict[str, Any]]:
    if not openai_async_client:
        logger.error("OpenAI client not initialized. Cannot extract information.")
        return {"type": "error", "reason": "OpenAI client not initialized."}

    user_prompt = LLM_PROMOTION_EXTRACTION_PROMPT_TEMPLATE.format(
        message_text=message_text
    )
    logger.debug(
        f"Sending to OpenAI (model: {config.OPENAI_MODEL_NAME}, first 100 chars of user prompt): {message_text[:100]}..."
    )

    raw_response_content = None
    try:
        response = await openai_async_client.chat.completions.create(
            model=config.OPENAI_MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant designed to output structured JSON according to the user's instructions. Output ONLY the JSON object.",
                },
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
        )

        if (
            response.choices
            and response.choices[0].message
            and response.choices[0].message.content
        ):
            raw_response_content = response.choices[0].message.content
            logger.trace(f"Raw response string from OpenAI: {raw_response_content}")
            extracted_data = json.loads(raw_response_content)
            return extracted_data
        else:
            logger.error(
                "OpenAI response structure is not as expected or content is missing."
            )
            return {"type": "error", "reason": "Unexpected OpenAI response structure."}

    except json.JSONDecodeError as e:
        logger.error(
            f"JSONDecodeError from OpenAI response: {e}. Response: '{str(raw_response_content)}'"
        )
        return {
            "type": "error",
            "reason": f"OpenAI response not valid JSON: {e}",
            "raw_response": str(raw_response_content),
        }
    except OpenAIError as e:
        logger.error(f"OpenAI API Error: {type(e).__name__} - {e}")
        return {"type": "error", "reason": f"OpenAI API Error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error calling OpenAI: {type(e).__name__} - {e}")
        return {"type": "error", "reason": f"OpenAI API call failed: {str(e)}"}


def extract_urls_from_text(text: Optional[str]) -> Optional[List[str]]:
    if not text:
        return None
    url_pattern = re.compile(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
        r"(?:[a-zA-Z0-9/])?"
    )
    urls = url_pattern.findall(text)
    cleaned_urls = []
    for url in urls:
        while url.endswith((".", ",", "!", "?", ")", "]")):
            url = url[:-1]
        cleaned_urls.append(url)
    return cleaned_urls if cleaned_urls else None


async def process_message_data(
    message: Message,
    chat_name: str,
    get_sender_display_name_func: Callable[[Optional[Union[User, Chat, Channel]]], str],
) -> Optional[Dict[str, Any]]:
    if not message or not hasattr(message, "id"):
        logger.warning("process_message_data: Invalid message or message without ID.")
        return None

    sender_name = get_sender_display_name_func(message.sender)
    media_type_str = (
        type(message.media).__name__.replace("MessageMedia", "")
        if message.media
        else None
    )
    message_text_content = message.text if hasattr(message, "text") else None
    urls_found_regex = extract_urls_from_text(message_text_content)

    current_chat_id = message.chat_id
    if current_chat_id is None:
        if hasattr(message.peer_id, "channel_id"):
            current_chat_id = message.peer_id.channel_id
        elif hasattr(message.peer_id, "chat_id"):
            current_chat_id = message.peer_id.chat_id
        elif hasattr(message.peer_id, "user_id"):
            current_chat_id = message.peer_id.user_id
        else:
            logger.error(f"Could not determine chat_id for message ID {message.id}")
            return None

    base_processed_data = {
        "message_id": message.id,
        "chat_id": current_chat_id,
        "chat_name": chat_name,
        "sender_id": message.sender_id,
        "sender_name": sender_name,
        "message_text": message_text_content,
        "message_date": message.date,
        "media_type": media_type_str,
        "extracted_urls_regex": urls_found_regex,
        "extracted_info": None,
    }

    if message_text_content and is_potentially_promotional(message_text_content):
        logger.info(
            f"Message {message.id} (chat: {chat_name}) seems promotional. Sending to OpenAI..."
        )
        openai_data = await extract_promotion_info_with_openai(message_text_content)
        base_processed_data["extracted_info"] = openai_data

        if (
            openai_data
            and isinstance(openai_data, dict)
            and "link" in openai_data
            and openai_data["link"]
        ):
            if not base_processed_data["extracted_urls_regex"]:
                base_processed_data["extracted_urls_regex"] = []
            if openai_data["link"] not in base_processed_data["extracted_urls_regex"]:
                base_processed_data["extracted_urls_regex"].append(openai_data["link"])

        extracted_type = (
            openai_data.get("type", "unknown_type") if openai_data else "no_openai_data"
        )
        logger.info(f"OpenAI for msg ID {message.id} returned type: {extracted_type}")

    elif message_text_content:
        logger.debug(
            f"Message {message.id} (chat: {chat_name}) did not pass promotional keyword filter. OpenAI will not be called."
        )
        base_processed_data["extracted_info"] = {
            "type": "skipped_pre_filter",
            "reason": "Not promotional by initial screening.",
        }
    else:
        logger.debug(
            f"Message {message.id} (chat: {chat_name}) has no text content. OpenAI will not be called."
        )
        base_processed_data["extracted_info"] = {
            "type": "no_text_content",
            "reason": "Message without text.",
        }

    return base_processed_data
