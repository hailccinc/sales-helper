"""
search.py — Smart welding product search with query parsing and token boosting.

Extracts structured tokens (diameter, alloy, packaging) from natural language queries,
then ranks results using weighted fuzzy matching + token bonuses.
"""

from __future__ import annotations

import re

import pandas as pd
from rapidfuzz import fuzz


# ── Query parser token types ──────────────────────────────

# Diameter patterns: "0.045" ".045" "1/16" "3/32" etc
_DIAMETER_PATTERNS = [
    # Decimal: 0.045, .045, 0.035
    (re.compile(r'\b0?\.(\d{3})\b'), lambda m: m.group(1)),           # → "045"
    # Fraction: 1/16, 3/32, 1/8, 5/32
    (re.compile(r'\b(\d+)/(\d+)\b'), lambda m: f'{m.group(1)} {m.group(2)}'),  # → "1 16"
]

# Alloy codes: 70S-6, ER70S-6, 308L, E7018, E6010, 4043, 5356, etc
_ALLOY_PATTERN = re.compile(
    r'\b(?:ER|E)?(\d{2,4}[A-Z]?S?-?\d{0,3}[A-Z]{0,3})\b',
    re.IGNORECASE,
)

# Known alloy families for boosting
_KNOWN_ALLOYS = {
    '70S6', '70S-6', '70S3', '70S-3', '80SD2', '80S-D2',
    '308L', '308', '309L', '309', '316L', '316', '317L',
    '7018', '7014', '7024', '6010', '6011', '6013',
    '4043', '5356', '5183', '5556', '4047', '4145',
    '2209', '2594', '71T1', '71T9', '71T8', '71T11',
    '80NI1', '80S',
}

# Packaging patterns: "33#", "33lb", "50lb", "spool", "drum", "550#", "1000#", "coil"
_PKG_WEIGHT_PATTERN = re.compile(r'\b(\d+)\s*(?:#|lbs?|lb)\b', re.IGNORECASE)
_PKG_TYPE_PATTERN = re.compile(
    r'\b(spool|drum|coil|basket|carton|tube|hermetic|vacpak|pallet|bulk|mini)\b',
    re.IGNORECASE,
)

# ── Abbreviation expansions (natural language → data codes) ──

_EXPANSIONS = [
    (re.compile(r'\bs6\b', re.I),             '70S 6'),
    (re.compile(r'\bs-6\b', re.I),            '70S 6'),
    (re.compile(r'\bs3\b', re.I),             '70S 3'),
    (re.compile(r'\bs-3\b', re.I),            '70S 3'),
    (re.compile(r'\bER70S[-.]?6\b', re.I),    '70S 6'),
    (re.compile(r'\b70S[-.]?6\b', re.I),      '70S 6'),
    (re.compile(r'\bER70S[-.]?3\b', re.I),    '70S 3'),
    (re.compile(r'\b70S[-.]?3\b', re.I),      '70S 3'),
    (re.compile(r'\bdual\s*shield\b', re.I),   'DS'),
    (re.compile(r'\bcoreshield\b', re.I),      'CS'),
    (re.compile(r'\batom\s*arc\b', re.I),      'AA'),
    (re.compile(r'\bspoolarc\b', re.I),        'SA'),
    (re.compile(r'\bsureweld\b', re.I),        'SUREWELD'),
    (re.compile(r'\bautoshield\b', re.I),      'AS'),
    (re.compile(r'\bcoreweld\b', re.I),        'CW'),
    (re.compile(r'\balcotec\b', re.I),         'ER'),
    (re.compile(r'\bshield[- ]?brite\b', re.I), 'SB'),
    (re.compile(r'\bexaton\b', re.I),          'EXATON'),
    (re.compile(r'\bspraymaster\b', re.I),     'SPRAY MASTER'),
    (re.compile(r'\bspray\s*master\b', re.I),  'SPRAY MASTER'),
    (re.compile(r'\btig\s*rod\b', re.I),       'TIG'),
    (re.compile(r'\btig\s*wire\b', re.I),      'TIG'),
    (re.compile(r'\bmig\s*wire\b', re.I),      'WELD'),
    (re.compile(r'\bstick\b', re.I),           'ELECTRODE'),
    (re.compile(r'\bsmaw\b', re.I),            'ELECTRODE'),
    (re.compile(r'\bfcaw\b', re.I),            'DS'),
    (re.compile(r'\bgmaw\b', re.I),            'WELD'),
    (re.compile(r'\bflux[- ]?core\b', re.I),   'DS'),
    (re.compile(r'\bself[- ]?shield\b', re.I),  'CS'),
    (re.compile(r'\brebel\b', re.I),           'REBEL'),
    (re.compile(r'\brobust\s*feed\b', re.I),   'ROBUST FEED'),
    (re.compile(r'\bcutmaster\b', re.I),       'CUTMASTER'),
    (re.compile(r'\bpurus\b', re.I),           'PURUS'),
    (re.compile(r'\boks\b', re.I),             'OK'),
    (re.compile(r'\brutilia\b', re.I),         'SUREWELD'),
]

_STOP_WORDS = {'WIRE', 'ROD', 'FILLER', 'WELDING', 'THE', 'A', 'AN', 'FOR', 'AND', 'WITH', 'OF'}


class ParsedQuery:
    """Structured representation of a search query."""

    def __init__(self, raw: str):
        self.raw = raw
        self.diameters: list[str] = []      # e.g. ["045"]
        self.alloys: list[str] = []         # e.g. ["308L"]
        self.pkg_weights: list[str] = []    # e.g. ["33"]
        self.pkg_types: list[str] = []      # e.g. ["spool"]
        self.tokens: list[str] = []         # remaining tokens
        self.normalized: str = ""           # full normalized query string


def parse_query(text: str) -> ParsedQuery:
    """Parse a natural-language welding product query into structured tokens."""
    pq = ParsedQuery(text)
    working = text.strip()
    if not working:
        return pq

    # Extract diameters first (before normalization changes them)
    for pattern, extractor in _DIAMETER_PATTERNS:
        for m in pattern.finditer(working):
            pq.diameters.append(extractor(m))

    # Extract packaging weights: 33#, 50lb, etc
    for m in _PKG_WEIGHT_PATTERN.finditer(working):
        pq.pkg_weights.append(m.group(1))

    # Extract packaging types: spool, drum, coil, etc
    for m in _PKG_TYPE_PATTERN.finditer(working):
        pq.pkg_types.append(m.group(1).upper())

    # Normalize the query for fuzzy matching
    norm = working.upper().strip()
    for pattern, replacement in _EXPANSIONS:
        norm = pattern.sub(replacement, norm)

    # 0.045 → 045, .045 → 045
    norm = re.sub(r'\b0?\.(\d{3})\b', r'\1', norm)
    # 3/32 → 3 32
    norm = re.sub(r'(\d)/(\d+)', r'\1 \2', norm)

    pq.normalized = norm

    # Extract alloy codes from normalized text
    for m in _ALLOY_PATTERN.finditer(norm):
        code = m.group(1).upper().replace('-', '')
        if code in _KNOWN_ALLOYS or len(code) >= 3:
            pq.alloys.append(m.group(1).upper())

    # Tokenize
    raw_tokens = re.findall(r'[A-Z0-9]+', norm)
    pq.tokens = [t for t in raw_tokens if t not in _STOP_WORDS and len(t) > 0]

    return pq


def _score_item(pq: ParsedQuery, desc: str, pn: str, enriched: str) -> float:
    """Score a single item against a parsed query."""
    desc_upper = desc.upper()
    pn_upper = pn.upper()
    enriched_upper = enriched.upper() if enriched else ""
    combined = f"{desc_upper} {pn_upper} {enriched_upper}"

    # Token hit scoring
    hits = 0
    for token in pq.tokens:
        if token in combined:
            hits += 1
    n_tokens = max(len(pq.tokens), 1)
    token_ratio = hits / n_tokens

    if token_ratio == 0:
        return 0.0

    # Fuzzy matching — best of all fields
    desc_fuzzy = fuzz.token_set_ratio(pq.normalized, desc_upper)
    pn_fuzzy = fuzz.partial_ratio(pq.normalized, pn_upper)
    enriched_fuzzy = fuzz.token_set_ratio(pq.normalized, enriched_upper) if enriched_upper else 0
    fuzzy_best = max(desc_fuzzy, pn_fuzzy, enriched_fuzzy)

    # Coverage penalty (query tokens vs description length)
    desc_tokens = re.findall(r'[A-Z0-9]+', desc_upper)
    coverage = min(n_tokens / max(len(desc_tokens), 1), 1.0)

    # Base score = 45% token hits + 30% fuzzy + 15% coverage
    score = (token_ratio * 45) + (fuzzy_best / 100 * 30) + (coverage * 15)

    # ── Token bonuses ──

    # Diameter match: huge boost if query diameter found in item
    if pq.diameters:
        diam_found = any(d in combined for d in pq.diameters)
        if diam_found:
            score += 15
        else:
            score -= 10  # penalty for wrong diameter

    # Alloy code match
    if pq.alloys:
        alloy_found = any(a.replace('-', '') in combined.replace('-', '') for a in pq.alloys)
        if alloy_found:
            score += 12
        else:
            score -= 8

    # Packaging weight match
    if pq.pkg_weights:
        pkg_found = any(f"{w}F" in combined or f"{w}LB" in combined or f"{w} LB" in combined
                        for w in pq.pkg_weights)
        if pkg_found:
            score += 8

    # Packaging type match
    if pq.pkg_types:
        type_found = any(t in combined for t in pq.pkg_types)
        if type_found:
            score += 5

    # All tokens hit = bonus
    if token_ratio == 1.0:
        score += 8

    # Exact part number match
    norm_query_flat = pq.normalized.replace(" ", "")
    if norm_query_flat == pn_upper.replace(" ", ""):
        score = 100

    return max(min(score, 100), 0)


def search_products(
    query: str,
    master_df: pd.DataFrame,
    max_results: int = 12,
    min_score: float = 30,
    enriched_col: str | None = "enriched_description",
) -> pd.DataFrame:
    """
    Smart search: parses query for structured tokens, then fuzzy-matches with boosting.
    Returns DataFrame with match_score column (0-100).
    """
    if master_df.empty or not query.strip():
        return pd.DataFrame()

    pq = parse_query(query)

    if not pq.tokens:
        return pd.DataFrame()

    descriptions = master_df["description"].fillna("").astype(str).tolist()
    part_numbers = master_df["part_number"].fillna("").astype(str).tolist()

    has_enriched = enriched_col and enriched_col in master_df.columns
    enriched_descs = (master_df[enriched_col].fillna("").astype(str).tolist()
                      if has_enriched else [""] * len(master_df))

    scores = [
        _score_item(pq, descriptions[i], part_numbers[i], enriched_descs[i])
        for i in range(len(master_df))
    ]

    result = master_df.copy()
    result["match_score"] = scores

    result = result[result["match_score"] >= min_score]
    result = result.sort_values("match_score", ascending=False)
    result = result.head(max_results)

    return result.reset_index(drop=True)
