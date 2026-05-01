"""GeoID matcher for resolving jurisdiction names to FIPS GEOIDs.

Matches external data source names (e.g., "Autauga County") to the
canonical GEOIDs in the jurisdictions table using exact, normalized,
and fuzzy matching strategies.
"""

import logging
import re
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from src.models.jurisdiction import Jurisdiction

logger = logging.getLogger("jurisdiction.geoid_matcher")

# Suffixes to strip during normalization
_NAME_SUFFIXES = [
    " County", " Parish", " Borough", " Census Area", " Municipality",
    " city", " town", " village", " borough", " township",
    " CDP", " plantation", " gore", " grant", " location",
    " purchase", " unorganized territory",
    " City", " Town", " Village", " Borough", " Township",
]


class GeoIDMatcher:
    """Matches jurisdiction names from external sources to GEOIDs.

    Builds an in-memory lookup from the jurisdictions table, then
    provides exact, normalized, and fuzzy matching.
    """

    def __init__(self, session: Session, census_year: int = 2023) -> None:
        self.session = session
        self.census_year = census_year
        self._lookup: Dict[str, List[Tuple[str, str, str]]] = {}
        self._build_lookup()

    def _build_lookup(self) -> None:
        """Build state-indexed lookup from jurisdictions table.

        Structure: {state_fips: [(geoid, jurisdiction_name, jurisdiction_type), ...]}
        """
        rows = (
            self.session.query(
                Jurisdiction.geoid,
                Jurisdiction.jurisdiction_name,
                Jurisdiction.jurisdiction_name_lsad,
                Jurisdiction.jurisdiction_type,
                Jurisdiction.state_fips,
            )
            .filter(Jurisdiction.census_year == self.census_year)
            .all()
        )

        for geoid, name, name_lsad, jtype, state_fips in rows:
            if state_fips not in self._lookup:
                self._lookup[state_fips] = []
            self._lookup[state_fips].append((geoid, name, name_lsad or name, jtype))

        logger.debug(
            f"Built GEOID lookup with {sum(len(v) for v in self._lookup.values())} "
            f"entries across {len(self._lookup)} states"
        )

    def match(
        self,
        state_fips: str,
        name: str,
        jurisdiction_type: Optional[str] = None,
        min_similarity: float = 0.85,
    ) -> Optional[str]:
        """Match a jurisdiction name to a GEOID.

        Tries in order:
        1. Exact match on jurisdiction_name or jurisdiction_name_lsad
        2. Normalized match (strip suffixes, lowercase)
        3. Fuzzy match using SequenceMatcher

        Args:
            state_fips: 2-digit state FIPS code.
            name: Jurisdiction name to match.
            jurisdiction_type: Optional type filter (county, municipality, mcd).
            min_similarity: Minimum similarity ratio for fuzzy matching.

        Returns:
            GEOID string if matched, None otherwise.
        """
        candidates = self._lookup.get(state_fips, [])
        if jurisdiction_type:
            candidates = [(g, n, nl, t) for g, n, nl, t in candidates if t == jurisdiction_type]

        if not candidates:
            return None

        # 1. Exact match
        for geoid, cand_name, cand_lsad, _ in candidates:
            if name == cand_name or name == cand_lsad:
                return geoid

        # 2. Normalized match
        norm_name = self._normalize(name)
        for geoid, cand_name, cand_lsad, _ in candidates:
            if norm_name == self._normalize(cand_name):
                return geoid
            if norm_name == self._normalize(cand_lsad):
                return geoid

        # 3. Fuzzy match
        best_geoid = None
        best_ratio = 0.0
        for geoid, cand_name, cand_lsad, _ in candidates:
            for cand in [cand_name, self._normalize(cand_name)]:
                ratio = SequenceMatcher(None, norm_name, self._normalize(cand)).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_geoid = geoid

        if best_ratio >= min_similarity:
            return best_geoid

        return None

    def match_batch(
        self,
        state_fips: str,
        names: List[str],
        jurisdiction_type: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """Match multiple names, returning {name: geoid_or_none}."""
        return {
            name: self.match(state_fips, name, jurisdiction_type)
            for name in names
        }

    @staticmethod
    def _normalize(name: str) -> str:
        """Normalize a name for comparison: strip suffixes, lowercase, remove punctuation."""
        result = name.strip()
        for suffix in _NAME_SUFFIXES:
            if result.endswith(suffix):
                result = result[: -len(suffix)]
                break
        # Remove common prefixes
        for prefix in ["City of ", "Town of ", "Village of "]:
            if result.startswith(prefix):
                result = result[len(prefix):]
                break
        # Lowercase and strip punctuation
        result = re.sub(r"[^\w\s]", "", result.lower())
        return result.strip()
