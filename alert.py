import requests

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

