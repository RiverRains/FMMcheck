import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class StateManager:
    def __init__(self, output_path):
        """Initialize with the path to the Excel output file to determine state file location."""
        # Check if output_path is a DBFS path
        path_str = str(output_path)
        if path_str.startswith("/dbfs/"):
            # Use the exact directory structure but change the extension
            base_dir = path_str.rsplit('/', 1)[0] if '/' in path_str else ''
            self.state_path = Path(base_dir) / "football_fetch_state.json"
        else:
            self.state_path = Path(output_path).parent / "football_fetch_state.json"

    def load_fetch_state(self):
        """
        Load state that tracks which match IDs were written last run and which the user has deleted.
        """
        if not self.state_path.exists():
            return {"last_written": {}, "deleted": {}}
        try:
            with open(self.state_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {
                "last_written": data.get("last_written", {}),
                "deleted": data.get("deleted", {})
            }
        except Exception as e:
            logger.warning(f"Could not load state file from {self.state_path} ({e}). Deleted-match tracking will not apply this run.")
            return {"last_written": {}, "deleted": {}}

    def save_fetch_state(self, state):
        """Save state after writing the Excel file."""
        try:
            # Ensure the directory exists
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
            logger.info(f"Successfully saved state to {self.state_path}")
        except Exception as e:
            logger.error(f"Could not save state file to {self.state_path} ({e}).")
