"""Semi-automated state research orchestrator.

Coordinates NABCA parsing, ABC website scraping, and manual verification
to build the 56-row state classification matrix.
"""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.common.constants import (
    CONTROL_STATES,
    FIPS_STATES,
    STRONG_MCD_STATES,
    TERRITORY_FIPS,
)
from src.common.enums import ControlStatus, ResearchStatus
from src.models.research_note import ResearchNote
from src.models.state_classification import StateClassification
from src.research.abc_scraper import ABCFindings, ABCScraper
from src.research.nabca_parser import NABCAFindings, NABCAParser
from src.research.research_prompts import generate_research_checklist

logger = logging.getLogger("jurisdiction.researcher")


class StateResearcher:
    """Semi-automated research tool for building the state classification matrix.

    Workflow:
        1. Run automated research (NABCA + ABC scraping) for all states
        2. Export a worksheet with findings + blank columns for manual verification
        3. Human fills in the worksheet
        4. Import the verified worksheet back into the database
    """

    def __init__(
        self,
        session: Session,
        nabca_parser: Optional[NABCAParser] = None,
        abc_scraper: Optional[ABCScraper] = None,
    ) -> None:
        self.session = session
        self.nabca_parser = nabca_parser or NABCAParser()
        self.abc_scraper = abc_scraper or ABCScraper()

    def research_state(
        self, state_fips: str, abc_url: Optional[str] = None
    ) -> Dict:
        """Run all automated research for a single state.

        Args:
            state_fips: 2-digit state FIPS code.
            abc_url: Optional URL for the state's ABC agency website.

        Returns:
            Dict with combined findings and a draft classification.
        """
        state_info = FIPS_STATES.get(state_fips)
        if not state_info:
            raise ValueError(f"Unknown state FIPS: {state_fips}")

        abbr, name = state_info
        logger.info(f"Researching {name} ({abbr}) [FIPS: {state_fips}]")

        # NABCA data
        nabca_findings = self.nabca_parser.get_state_summary(state_fips)

        # ABC website scraping
        abc_findings = self.abc_scraper.scrape_state(state_fips, abc_url)

        # Synthesize findings into a draft classification
        draft = self._synthesize_draft(state_fips, nabca_findings, abc_findings)

        # Record research notes
        self._record_notes(state_fips, nabca_findings, abc_findings)

        return {
            "state_fips": state_fips,
            "state_abbr": abbr,
            "state_name": name,
            "nabca_findings": nabca_findings,
            "abc_findings": abc_findings,
            "draft_classification": draft,
        }

    def research_all_states(self) -> pd.DataFrame:
        """Run research for all 56 jurisdictions and return a summary DataFrame."""
        rows = []
        for fips in FIPS_STATES:
            result = self.research_state(fips)
            draft = result["draft_classification"]
            rows.append(draft)

        return pd.DataFrame(rows)

    def export_research_worksheet(self, output_path: Path) -> None:
        """Export a CSV worksheet with automated findings and blank verification columns.

        The worksheet has pre-filled columns from automated research and
        empty columns for manual verification.
        """
        rows = []
        for fips, (abbr, name) in FIPS_STATES.items():
            nabca = self.nabca_parser.get_state_summary(fips)

            row = {
                "state_fips": fips,
                "state_abbr": abbr,
                "state_name": name,
                "is_territory": fips in TERRITORY_FIPS,
                # Pre-filled from automated research
                "auto_control_status": "control" if nabca.is_control_state else "license",
                "auto_is_strong_mcd": fips in STRONG_MCD_STATES,
                "auto_confidence": nabca.confidence,
                # Blank columns for manual verification
                "verified_control_status": "",
                "verified_has_local_licensing": "",
                "verified_delegates_to_county": "",
                "verified_delegates_to_municipality": "",
                "verified_delegates_to_mcd": "",
                "verified_has_local_option_law": "",
                "verified_local_option_level": "",
                "abc_agency_name": "",
                "abc_agency_url": "",
                "research_source": "",
                "research_notes": "",
                "research_status": "pending",
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported research worksheet to {output_path} ({len(rows)} rows)")

    def import_verified_worksheet(self, input_path: Path) -> int:
        """Import a manually-verified worksheet into the state_classifications table.

        Args:
            input_path: Path to the verified CSV worksheet.

        Returns:
            Number of rows imported.
        """
        df = pd.read_csv(input_path, dtype=str)
        df = df.fillna("")

        count = 0
        for _, row in df.iterrows():
            # Use verified values if available, fall back to auto values
            control_status = (
                row.get("verified_control_status")
                or row.get("auto_control_status")
                or "license"
            )
            has_local = self._parse_bool(row.get("verified_has_local_licensing", ""))
            delegates_county = self._parse_bool(row.get("verified_delegates_to_county", ""))
            delegates_muni = self._parse_bool(row.get("verified_delegates_to_municipality", ""))
            delegates_mcd = self._parse_bool(row.get("verified_delegates_to_mcd", ""))
            has_local_option = self._parse_bool(row.get("verified_has_local_option_law", ""))

            classification = StateClassification(
                state_fips=row["state_fips"],
                state_abbr=row["state_abbr"],
                state_name=row["state_name"],
                is_territory=row.get("is_territory", "").lower() == "true",
                control_status=control_status,
                has_local_licensing=has_local,
                delegates_to_county=delegates_county,
                delegates_to_municipality=delegates_muni,
                delegates_to_mcd=delegates_mcd,
                is_strong_mcd_state=row.get("auto_is_strong_mcd", "").lower() == "true",
                has_local_option_law=has_local_option,
                local_option_level=row.get("verified_local_option_level") or None,
                abc_agency_name=row.get("abc_agency_name") or None,
                abc_agency_url=row.get("abc_agency_url") or None,
                research_status=row.get("research_status", "pending"),
                research_source=row.get("research_source") or None,
                research_notes=row.get("research_notes") or None,
            )

            self.session.merge(classification)
            count += 1

        self.session.commit()
        logger.info(f"Imported {count} state classifications from {input_path}")
        return count

    def _synthesize_draft(
        self,
        state_fips: str,
        nabca: NABCAFindings,
        abc: ABCFindings,
    ) -> Dict:
        """Combine automated findings into a draft classification dict."""
        abbr, name = FIPS_STATES[state_fips]

        return {
            "state_fips": state_fips,
            "state_abbr": abbr,
            "state_name": name,
            "is_territory": state_fips in TERRITORY_FIPS,
            "control_status": "control" if nabca.is_control_state else "license",
            "has_local_licensing": "county_delegation_likely" in abc.delegation_hints
            or "municipal_delegation_likely" in abc.delegation_hints,
            "delegates_to_county": "county_delegation_likely" in abc.delegation_hints,
            "delegates_to_municipality": "municipal_delegation_likely" in abc.delegation_hints,
            "delegates_to_mcd": "mcd_delegation_likely" in abc.delegation_hints,
            "is_strong_mcd_state": state_fips in STRONG_MCD_STATES,
            "has_local_option_law": "local_option_laws_present" in abc.delegation_hints,
            "abc_findings_confidence": abc.confidence,
            "nabca_confidence": nabca.confidence,
            "research_status": "draft",
        }

    def _record_notes(
        self,
        state_fips: str,
        nabca: NABCAFindings,
        abc: ABCFindings,
    ) -> None:
        """Record research notes in the database."""
        # NABCA note
        note = ResearchNote(
            state_fips=state_fips,
            source_type="nabca",
            finding=nabca.control_notes or "No NABCA data available",
            confidence=nabca.confidence,
            researcher="automated",
        )
        self.session.add(note)

        # ABC scraper notes
        if abc.raw_snippets:
            for snippet in abc.raw_snippets[:5]:  # Limit to 5 snippets per state
                note = ResearchNote(
                    state_fips=state_fips,
                    source_url=abc.abc_url,
                    source_type="state_abc_website",
                    finding=snippet,
                    confidence=abc.confidence,
                    researcher="automated",
                )
                self.session.add(note)

    @staticmethod
    def _parse_bool(value: str) -> bool:
        """Parse a string to boolean, defaulting to False."""
        return value.strip().lower() in ("true", "yes", "1", "y")
