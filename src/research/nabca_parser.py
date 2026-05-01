"""Parser for NABCA (National Alcohol Beverage Control Association) data.

Extracts state-by-state regulatory classification from NABCA publications
and survey data to provide initial hints for the state classification matrix.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from src.common.constants import CONTROL_STATES, FIPS_STATES

logger = logging.getLogger("jurisdiction.nabca")


@dataclass
class NABCAFindings:
    """Findings from NABCA data for a single state."""

    state_fips: str
    state_name: str
    is_control_state: Optional[bool] = None
    control_notes: str = ""
    has_state_stores: Optional[bool] = None
    delegation_hints: List[str] = field(default_factory=list)
    raw_text: str = ""
    confidence: str = "low"


class NABCAParser:
    """Parses NABCA state regulatory data for classification hints."""

    # Known control states from NABCA (pre-populated from research)
    NABCA_CONTROL_STATES = CONTROL_STATES

    def __init__(self, request_delay: float = 2.0) -> None:
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "JurisdictionTaxonomy/1.0 (regulatory research tool)",
            }
        )

    def get_state_summary(self, state_fips: str) -> NABCAFindings:
        """Get NABCA classification findings for a state.

        Uses pre-populated knowledge from NABCA publications as the primary
        source, with optional web scraping for additional details.

        Args:
            state_fips: 2-digit state FIPS code.

        Returns:
            NABCAFindings with classification hints.
        """
        state_info = FIPS_STATES.get(state_fips)
        if not state_info:
            raise ValueError(f"Unknown state FIPS: {state_fips}")

        abbr, name = state_info
        is_control = state_fips in self.NABCA_CONTROL_STATES

        findings = NABCAFindings(
            state_fips=state_fips,
            state_name=name,
            is_control_state=is_control,
            control_notes=f"{'Control' if is_control else 'License'} state per NABCA classification",
            confidence="medium",
        )

        # Classify control state subtypes
        if is_control:
            state_store_only = {"01", "16", "33", "37", "42", "49", "51"}
            findings.has_state_stores = state_fips in state_store_only
            if findings.has_state_stores:
                findings.control_notes += " (state-run retail stores)"
            else:
                findings.control_notes += " (state controls wholesale, private retail allowed)"

        return findings

    def get_all_state_summaries(self) -> Dict[str, NABCAFindings]:
        """Get NABCA findings for all 56 jurisdictions.

        Returns:
            Dict mapping state FIPS to NABCAFindings.
        """
        results = {}
        for fips in FIPS_STATES:
            results[fips] = self.get_state_summary(fips)
        return results

    def scrape_nabca_page(self, url: str) -> Optional[str]:
        """Scrape a NABCA web page for regulatory text.

        Args:
            url: URL to scrape.

        Returns:
            Extracted text content, or None if request fails.
        """
        try:
            import time

            time.sleep(self.request_delay)
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract main content text
            main_content = soup.find("main") or soup.find("article") or soup.find("body")
            if main_content:
                return main_content.get_text(separator=" ", strip=True)
            return None
        except requests.RequestException as e:
            logger.warning(f"Failed to scrape {url}: {e}")
            return None
