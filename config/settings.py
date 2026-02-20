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

def get_dbutils():
    """Attempt to get the Databricks dbutils object if running in Databricks."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        # This is a common way to get dbutils in a Databricks notebook environment
        # Or using IPython
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if dbutils:
                return dbutils
        except Exception:
            pass
        
        # Another fallback
        if hasattr(spark, "conf") and "spark.databricks.workspaceUrl" in spark.conf.get("spark.app.name", ""):
            pass # we are likely in databricks
        
        # Assuming dbutils is available in the global scope in Databricks notebooks
        import builtins
        if hasattr(builtins, "dbutils"):
            return getattr(builtins, "dbutils")

        return None
    except Exception:
        return None

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
