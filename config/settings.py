import os
import sys
import logging
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# Default config
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#notifications-fmm")
SLACK_TOKEN_FILE = Path(__file__).resolve().parent.parent / ".slack_bot_token"


def get_bool_env(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_int_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; falling back to %s", name, value, default)
        return default


def should_notify_resolved_issues():
    return get_bool_env("SLACK_NOTIFY_RESOLVED", default=False)


def get_notification_state_retention_days():
    return max(1, get_int_env("NOTIFICATION_STATE_RETENTION_DAYS", default=14))

def get_api_key():
    """Get the Genius Sports API key from environment."""
    api_key = os.getenv("GENIUS_API_KEY", "").strip()
    if api_key:
        return api_key

    if sys.stdin.isatty():
        try:
            return input("Please enter your Genius Sports API Key: ").strip()
        except EOFError:
            pass

    logger.error("API key is required but not found in environment (GENIUS_API_KEY)!")
    return None

def get_slack_token():
    """Get the Slack bot token from environment."""
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    if token:
        return token

    if SLACK_TOKEN_FILE.exists():
        try:
            return SLACK_TOKEN_FILE.read_text(encoding="utf-8").strip().splitlines()[0].strip()
        except Exception as e:
            logger.debug(f"Failed to read slack token from file: {e}")

    return ""

def setup_logging():
    """Configure basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
