import requests
import os
import json
import re
import core

logger = None

def send_message(bot_token: str, chat_id: str, message: str) -> None:
    try:
        res = requests.get(
            url="https://api.telegram.org/bot%s/sendMessage" % bot_token,
            params={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "MarkdownV2",
            }
        )
    except Exception as e:
        logger.exception(
            "ERR_SEND_MESSAGE -> Chat ID: %s| Error: %s" % (chat_id, message))
    else:
        res_json = res.json()
        if "error_code" in res_json:
            logger.debug("WARN_SEND_MESSAGE -> Code: %s | Description: %s" % (
                res_json["error_code"], res_json["description"]
            ))
        else:
            logger.debug("ALERT REQUEST SENT -> %s" % chat_id)
        return res_json

def escape_markdown_v2(text):
    escape_chars = r'_\*\[\]()~`>#+\-=|{}.!'
    return re.sub(f'([{escape_chars}])', r'\\\1', text)

def send_country_message():
    """Send message from remove_country_message.txt file to all configured chat IDs"""
    message_file = 'remove_country_message.txt'
    if os.path.exists(message_file):
        with open(message_file, 'r', encoding='utf-8') as f:
            message = f.read()
        if not message.strip():
            print("Empty Message")
            return
        chat_ids = core.load_chatids()
        # In case of failure during loading latest chatids for unknown reason,
        # it will use the previously loaded chatids in starting of the script
        if not chat_ids:
            chat_ids = core.chat_ids

        # Escape message for MarkdownV2
        message = escape_markdown_v2(message)

        for chat_id in chat_ids:
            _ = send_message(
                bot_token=core.bot_token,
                chat_id=chat_id,
                message=message
            )
        # Set the message file as empty after sending
        with open(message_file, 'w', encoding='utf-8') as f:
            f.write('')
    else:
        logger.debug("remove_country_message.txt does not exist!!")

