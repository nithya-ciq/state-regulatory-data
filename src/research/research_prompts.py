"""Structured prompts and checklists for manual state research.

Generates per-state research worksheets to guide the human verification
process for the state classification matrix.
"""

import logging
from typing import Dict, List

from src.common.constants import CONTROL_STATES, FIPS_STATES, STRONG_MCD_STATES, TERRITORY_FIPS

logger = logging.getLogger("jurisdiction.research")


def generate_research_checklist(state_fips: str) -> List[Dict[str, str]]:
    """Generate a structured research checklist for a single state.

    Args:
        state_fips: 2-digit state FIPS code.

    Returns:
        List of question dicts with 'question', 'field', and 'hint' keys.
    """
    state_info = FIPS_STATES.get(state_fips)
    if not state_info:
        raise ValueError(f"Unknown state FIPS: {state_fips}")

    abbr, name = state_info
    is_territory = state_fips in TERRITORY_FIPS
    is_control = state_fips in CONTROL_STATES
    is_strong_mcd = state_fips in STRONG_MCD_STATES

    questions = [
        {
            "question": f"Is {name} a control state or license state for alcohol?",
            "field": "control_status",
            "hint": f"Pre-filled: {'control' if is_control else 'license'} "
            f"(verify against NABCA data)",
        },
        {
            "question": f"Does {name} delegate any alcohol licensing authority to local jurisdictions?",
            "field": "has_local_licensing",
            "hint": "Look for local licensing pages on the state ABC website",
        },
        {
            "question": f"Do counties in {name} issue alcohol licenses?",
            "field": "delegates_to_county",
            "hint": "Check if county boards/commissions approve or issue licenses",
        },
        {
            "question": f"Do municipalities (cities/towns/villages) in {name} issue alcohol licenses?",
            "field": "delegates_to_municipality",
            "hint": "Check if city councils or municipal boards approve licenses",
        },
    ]

    if is_strong_mcd:
        questions.append(
            {
                "question": f"Do townships/towns in {name} have alcohol licensing authority?",
                "field": "delegates_to_mcd",
                "hint": f"{name} is a strong-MCD state with active township governance",
            }
        )

    questions.extend(
        [
            {
                "question": f"Does {name} have local option laws (allowing dry/wet votes)?",
                "field": "has_local_option_law",
                "hint": "Look for 'local option', 'dry county', 'wet/dry' references",
            },
            {
                "question": f"At what level can local option votes occur in {name}?",
                "field": "local_option_level",
                "hint": "Options: county, municipality, precinct, or NULL if no local option",
            },
            {
                "question": f"What is the name of {name}'s ABC/alcohol regulatory agency?",
                "field": "abc_agency_name",
                "hint": "The primary state agency overseeing alcohol regulation",
            },
            {
                "question": f"What is the website URL for {name}'s ABC agency?",
                "field": "abc_agency_url",
                "hint": "Main website for the state alcohol regulatory body",
            },
        ]
    )

    return questions


def generate_all_checklists() -> Dict[str, List[Dict[str, str]]]:
    """Generate research checklists for all 56 jurisdictions.

    Returns:
        Dict mapping state FIPS to list of research questions.
    """
    return {fips: generate_research_checklist(fips) for fips in FIPS_STATES}


def format_checklist_text(state_fips: str) -> str:
    """Format a research checklist as readable text for console output.

    Args:
        state_fips: 2-digit state FIPS code.

    Returns:
        Formatted text string.
    """
    state_info = FIPS_STATES.get(state_fips)
    if not state_info:
        return f"Unknown state FIPS: {state_fips}"

    abbr, name = state_info
    questions = generate_research_checklist(state_fips)

    lines = [
        f"=== Research Checklist: {name} ({abbr}) [FIPS: {state_fips}] ===",
        "",
    ]

    for i, q in enumerate(questions, 1):
        lines.append(f"  {i}. {q['question']}")
        lines.append(f"     Field: {q['field']}")
        lines.append(f"     Hint: {q['hint']}")
        lines.append("")

    return "\n".join(lines)
