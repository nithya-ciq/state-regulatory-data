"""Download manifest tracker for Census TIGER data.

Tracks which state/layer/year combinations have been downloaded
to avoid redundant downloads across pipeline runs.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("jurisdiction.cache")


class DownloadManifest:
    """Tracks Census data download status in a local JSON manifest file."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.manifest_path = cache_dir / "download_manifest.json"
        self._manifest: Dict[str, Dict] = self._load()

    def _load(self) -> Dict[str, Dict]:
        """Load manifest from disk."""
        if self.manifest_path.exists():
            with open(self.manifest_path, "r") as f:
                return json.load(f)
        return {}

    def _save(self) -> None:
        """Persist manifest to disk."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(self._manifest, f, indent=2, default=str)

    @staticmethod
    def _make_key(state_fips: str, geo_layer: str, year: int) -> str:
        """Create a unique key for a state/layer/year combination."""
        return f"{state_fips}_{geo_layer}_{year}"

    def is_downloaded(self, state_fips: str, geo_layer: str, year: int) -> bool:
        """Check if data for this state/layer/year has been downloaded."""
        key = self._make_key(state_fips, geo_layer, year)
        return key in self._manifest

    def mark_downloaded(
        self,
        state_fips: str,
        geo_layer: str,
        year: int,
        row_count: int,
    ) -> None:
        """Record a successful download."""
        key = self._make_key(state_fips, geo_layer, year)
        self._manifest[key] = {
            "state_fips": state_fips,
            "geo_layer": geo_layer,
            "year": year,
            "row_count": row_count,
            "downloaded_at": datetime.utcnow().isoformat(),
        }
        self._save()
        logger.debug(f"Marked {key} as downloaded ({row_count} rows)")

    def get_download_info(
        self, state_fips: str, geo_layer: str, year: int
    ) -> Optional[Dict]:
        """Get download metadata for a state/layer/year combination."""
        key = self._make_key(state_fips, geo_layer, year)
        return self._manifest.get(key)

    def clear(self) -> None:
        """Clear all download records."""
        self._manifest = {}
        self._save()
        logger.info("Download manifest cleared")
