"""Scraper for state ABC (Alcohol Beverage Control) agency websites.

Scrapes each state's ABC agency website looking for evidence of
local licensing delegation patterns.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from src.common.constants import FIPS_STATES

logger = logging.getLogger("jurisdiction.abc_scraper")


# Delegation keywords to search for in page text
DELEGATION_KEYWORDS = {
    "county_license": [
        "county license", "county permit", "county board", "county commission",
        "county authority", "county approval", "county ordinance",
    ],
    "municipal_license": [
        "municipal license", "municipal permit", "city license", "city permit",
        "city council approval", "municipality", "municipal authority",
        "village license", "town license",
    ],
    "township_authority": [
        "township license", "township permit", "township authority",
        "town meeting", "town board",
    ],
    "local_option": [
        "local option", "dry county", "wet county", "moist county",
        "dry jurisdiction", "local referendum", "local vote",
        "wet/dry", "dry/wet",
    ],
    "state_only": [
        "state-issued only", "all licenses issued by", "sole licensing authority",
        "centralized licensing",
    ],
}


@dataclass
class ABCFindings:
    """Findings from scraping a state ABC agency website."""

    state_fips: str
    state_name: str
    abc_url: Optional[str] = None
    pages_scraped: int = 0
    keyword_matches: Dict[str, List[str]] = field(default_factory=dict)
    delegation_hints: List[str] = field(default_factory=list)
    raw_snippets: List[str] = field(default_factory=list)
    confidence: str = "low"


class ABCScraper:
    """Scrapes state ABC agency websites for delegation pattern evidence."""

    def __init__(self, request_delay: float = 2.0) -> None:
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "JurisdictionTaxonomy/1.0 (regulatory research tool)",
            }
        )

    def scrape_state(self, state_fips: str, abc_url: Optional[str] = None) -> ABCFindings:
        """Scrape a state's ABC agency website for delegation evidence.

        Args:
            state_fips: 2-digit state FIPS code.
            abc_url: URL of the state ABC agency website. If None, attempts lookup.

        Returns:
            ABCFindings with keyword matches and delegation hints.
        """
        state_info = FIPS_STATES.get(state_fips)
        if not state_info:
            raise ValueError(f"Unknown state FIPS: {state_fips}")

        abbr, name = state_info
        findings = ABCFindings(state_fips=state_fips, state_name=name, abc_url=abc_url)

        if not abc_url:
            logger.info(f"No ABC URL provided for {name} ({state_fips}), skipping scrape")
            return findings

        # Scrape the main page
        page_text = self._fetch_page_text(abc_url)
        if page_text:
            findings.pages_scraped += 1
            self._analyze_text(page_text, findings)

        # Try common subpages
        for subpath in ["/licensing", "/permits", "/local", "/local-licensing", "/faq"]:
            sub_url = abc_url.rstrip("/") + subpath
            sub_text = self._fetch_page_text(sub_url)
            if sub_text:
                findings.pages_scraped += 1
                self._analyze_text(sub_text, findings)

        # Derive confidence based on evidence
        total_matches = sum(len(v) for v in findings.keyword_matches.values())
        if total_matches >= 5:
            findings.confidence = "medium"
        if total_matches >= 10:
            findings.confidence = "high"

        return findings

    def _fetch_page_text(self, url: str) -> Optional[str]:
        """Fetch and extract text from a URL."""
        try:
            time.sleep(self.request_delay)
            response = self.session.get(url, timeout=30, allow_redirects=True)
            if response.status_code != 200:
                return None
            soup = BeautifulSoup(response.text, "html.parser")

            # Remove script and style elements
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()

            return soup.get_text(separator=" ", strip=True)
        except requests.RequestException as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

    def _analyze_text(self, text: str, findings: ABCFindings) -> None:
        """Search text for delegation keywords and extract matching snippets."""
        text_lower = text.lower()

        for category, keywords in DELEGATION_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_lower:
                    if category not in findings.keyword_matches:
                        findings.keyword_matches[category] = []
                    findings.keyword_matches[category].append(keyword)

                    # Extract a snippet around the match
                    snippet = self._extract_snippet(text, keyword, context_chars=150)
                    if snippet:
                        findings.raw_snippets.append(f"[{category}] {snippet}")

        # Derive delegation hints from keyword categories found
        if "county_license" in findings.keyword_matches:
            findings.delegation_hints.append("county_delegation_likely")
        if "municipal_license" in findings.keyword_matches:
            findings.delegation_hints.append("municipal_delegation_likely")
        if "township_authority" in findings.keyword_matches:
            findings.delegation_hints.append("mcd_delegation_likely")
        if "local_option" in findings.keyword_matches:
            findings.delegation_hints.append("local_option_laws_present")
        if "state_only" in findings.keyword_matches:
            findings.delegation_hints.append("state_only_licensing_likely")

    @staticmethod
    def _extract_snippet(text: str, keyword: str, context_chars: int = 150) -> Optional[str]:
        """Extract a text snippet around a keyword match."""
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
        match = pattern.search(text)
        if not match:
            return None

        start = max(0, match.start() - context_chars)
        end = min(len(text), match.end() + context_chars)
        snippet = text[start:end].strip()

        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."

        return snippet
