# Sales Helper

Product lookup and pricing tool for ESAB distributors. Search by natural language description or part number, see pricing with precedence logic, and find packaging variants.

## Setup

### Prerequisites
- Python 3.10+
- Price files in `~/Desktop/sales app/`

### Install

```bash
cd ~/sales-helper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows:
```powershell
cd ~\sales-helper
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Run

```bash
source .venv/bin/activate
streamlit run app.py
```

Opens at http://localhost:8501

## Price File Setup

Place price files in `~/Desktop/sales app/`. Supported formats: `.numbers`, `.xlsx`, `.csv`.

**File classification** (by filename keywords in `config/pricing_rules.yaml`):

| Keyword | Classification |
|---|---|
| `welsco`, `nexair` | Welsco/Nexair distributor |
| `red ball`, `redball` | Red Ball Oxygen distributor |
| `pricelist`, `master`, `base` | Master price list |
| `eup`, `end user`, `special` | End-user pricing |
| `location`, `alcotec special` | Location-specific pricing |

Files without a distributor keyword are classified by their PAYER column.

## Pricing Precedence

1. **Location Special** (highest priority)
2. **End User**
3. **Master Tier**
4. **Master List** (lowest)

Surcharges (alloy, tariff) are added on top of special prices.

## Description Enrichment

Product descriptions are enriched in two layers:

1. **Pattern decoder** (instant) — Translates ESAB abbreviated codes into human-readable text.
   - `WELD 70S 6 045X44F` becomes `Spoolarc Weld 70S-6 | MIG Wire (ER70S-6) | 0.045" | 44 lb`
   - `AA 7018 1 8X14X50FHS` becomes `Atom Arc 7018 | Stick Electrode (E7018) | 1/8" | 14" x 50 lb Hermetically Sealed`
   - Covers ~46% of items automatically

2. **Web search** (optional batch) — Searches DuckDuckGo for product families and caches results.

### Running web enrichment

```bash
source .venv/bin/activate

# Full run (all product families, ~1-2 hours)
python scripts/enrich_descriptions.py

# First 100 families only
python scripts/enrich_descriptions.py --limit 100

# Skip already-cached families
python scripts/enrich_descriptions.py --resume

# Preview without searching
python scripts/enrich_descriptions.py --dry-run
```

Results are cached in `data/descriptions.json` and loaded automatically by the app.

## Configuration

Edit `config/pricing_rules.yaml` to adjust:
- Distributor classification keywords
- List type classification keywords
- Column header mappings (exact + contains matching)
- Search settings (max results, minimum score)
