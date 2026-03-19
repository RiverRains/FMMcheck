import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _default_notification_state():
    return {"open_issues": {}, "resolved_issues": {}}


def _parse_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def prune_resolved_issues(resolved_issues, retention_days, now=None):
    """Drop resolved issues older than the retention window."""
    if retention_days <= 0:
        return {}

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)
    pruned = {}

    for issue_key, issue in (resolved_issues or {}).items():
        if not isinstance(issue, dict):
            continue

        resolved_at = _parse_timestamp(issue.get("resolved_at"))
        if resolved_at is None or resolved_at >= cutoff:
            pruned[issue_key] = issue

    return pruned


class NotificationStateManager:
    def __init__(self, output_path):
        """Store notification state beside the configured output file."""
        path_str = str(output_path)
        if path_str.startswith("/dbfs/"):
            base_dir = path_str.rsplit("/", 1)[0] if "/" in path_str else ""
            self.state_path = Path(base_dir) / "notification_state.json"
        else:
            self.state_path = Path(output_path).parent / "notification_state.json"

    def load_state(self):
        if not self.state_path.exists():
            return _default_notification_state()

        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(
                "Could not load notification state from %s (%s). Notification dedupe will reset this run.",
                self.state_path,
                e,
            )
            return _default_notification_state()

        open_issues = data.get("open_issues", {})
        resolved_issues = data.get("resolved_issues", {})

        if not isinstance(open_issues, dict):
            open_issues = {}
        if not isinstance(resolved_issues, dict):
            resolved_issues = {}

        return {
            "open_issues": open_issues,
            "resolved_issues": resolved_issues,
        }

    def save_state(self, state):
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, sort_keys=True)
            logger.info("Successfully saved notification state to %s", self.state_path)
        except Exception as e:
            logger.error("Could not save notification state to %s (%s).", self.state_path, e)
