import logging
from config.settings import get_slack_token, SLACK_CHANNEL

logger = logging.getLogger(__name__)

def send_slack_message(text, channel=None, blocks=None):
    """Post message to Slack. Returns True if sent, False if no token or send failed."""
    token = get_slack_token()
    if not token:
        logger.warning("No Slack token found. Skipping Slack notification.")
        return False
        
    channel = channel or SLACK_CHANNEL
    
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token)
        kwargs = {"channel": channel, "text": text}
        if blocks:
            kwargs["blocks"] = blocks
        client.chat_postMessage(**kwargs)
        return True
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
        return False
