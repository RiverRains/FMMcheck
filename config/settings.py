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

def get_dbutils():
    """Attempt to get the Databricks dbutils object if running in Databricks."""
    try:
        # Global scope (notebooks)
        import builtins
        if hasattr(builtins, "dbutils"):
            return getattr(builtins, "dbutils")
        # IPython (notebooks)
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if dbutils:
                return dbutils
        except Exception:
            pass
        # Spark session + DBUtils (job contexts that have Spark)
        try:
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils
            spark = SparkSession.builder.getOrCreate()
            return DBUtils(spark)
        except Exception:
            pass
        return None
    except Exception:
        return None


def inject_databricks_secrets_into_env():
    """
    When running in Databricks (notebook or job with dbutils), load secrets from
    fmm_scope into os.environ so the rest of the app can use get_api_key() etc.
    without needing UI-configured environment variables. Safe to call when not
    in Databricks or when env vars are already set (no-op).
    """
    if os.getenv("GENIUS_API_KEY") and os.getenv("SLACK_BOT_TOKEN"):
        return
    dbutils = get_dbutils()
    if not dbutils:
        return
    scope = "fmm_scope"
    try:
        if not os.getenv("GENIUS_API_KEY"):
            val = dbutils.secrets.get(scope=scope, key="genius_api_key")
            if val:
                os.environ["GENIUS_API_KEY"] = val
    except Exception as e:
        logger.debug("Could not load genius_api_key from Databricks secrets: %s", e)
    try:
        if not os.getenv("SLACK_BOT_TOKEN"):
            val = dbutils.secrets.get(scope=scope, key="slack_bot_token")
            if val:
                os.environ["SLACK_BOT_TOKEN"] = val
    except Exception as e:
        logger.debug("Could not load slack_bot_token from Databricks secrets: %s", e)

def get_secret(scope, key, env_var=None):
    """
    Attempt to get a secret from Databricks.
    Fallback to environment variable if not in Databricks or secret not found.
    """
    dbutils = get_dbutils()
    if dbutils:
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception as e:
            logger.debug(f"Could not get secret {key} from scope {scope} in Databricks: {e}")
    
    # Fallback to environment variable
    if env_var:
        val = os.getenv(env_var)
        if val:
            return val.strip()
    return None

def get_api_key():
    """Get the Genius Sports API key."""
    # 1. Try Databricks secret
    # 2. Try Environment variable
    api_key = get_secret(scope="fmm_scope", key="genius_api_key", env_var="GENIUS_API_KEY")
    
    if api_key:
        return api_key

    # For local testing fallback, use input() if we're not running in Databricks
    if not get_dbutils() and sys.stdin.isatty():
        try:
            return input("Please enter your Genius Sports API Key: ").strip()
        except EOFError:
            pass

    logger.error("API key is required but not found in secrets or environment (GENIUS_API_KEY)!")
    return None

def get_slack_token():
    """Get the Slack bot token."""
    # 1. Try Databricks secret
    # 2. Try Environment variable
    token = get_secret(scope="fmm_scope", key="slack_bot_token", env_var="SLACK_BOT_TOKEN")
    
    if token:
        return token
        
    # 3. Try local file fallback
    if SLACK_TOKEN_FILE.exists():
        try:
            return SLACK_TOKEN_FILE.read_text(encoding="utf-8").strip().splitlines()[0].strip()
        except Exception as e:
            logger.debug(f"Failed to read slack token from file: {e}")
            pass
            
    return ""

def setup_logging():
    """Configure basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
