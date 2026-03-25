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

        # Migrate legacy format: open_issue_keys (array) → open_issues (dict)
        if not open_issues and "open_issue_keys" in data:
            legacy_keys = data["open_issue_keys"]
            if isinstance(legacy_keys, list):
                logger.info("Migrating %d legacy open_issue_keys to open_issues dict.", len(legacy_keys))
                updated_at = data.get("updated_at", datetime.now(timezone.utc).isoformat())
                open_issues = {
                    key: {"first_seen": updated_at, "last_seen": updated_at}
                    for key in legacy_keys
                }

        if not isinstance(open_issues, dict):
            open_issues = {}
        if not isinstance(resolved_issues, dict):
            resolved_issues = {}

        return {
            "open_issues": open_issues,
            "resolved_issues": resolved_issues,
        }

    def ensure_state_file(self):
        """Create an empty notification state file when it does not exist yet."""
        if self.state_path.exists():
            return
        self.save_state(_default_notification_state())

    def save_state(self, state):
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, sort_keys=True)
            logger.info("Successfully saved notification state to %s", self.state_path)
        except Exception as e:
            logger.error("Could not save notification state to %s (%s).", self.state_path, e)
