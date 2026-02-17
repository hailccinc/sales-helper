"""
loader.py — Discover, read, classify, and normalize price files.

Supports .numbers (Apple), .xlsx, and .csv files.
Uses config/pricing_rules.yaml for classification keywords and column mappings.

Two loading modes:
- load_all(): Scans ~/Desktop/sales-app/ for local files
- load_from_uploads(): Processes Streamlit UploadedFile objects (for cloud deployment)
"""

from __future__ import annotations

import io
import tempfile
import warnings
from pathlib import Path

import pandas as pd
import yaml


# ── Config ─────────────────────────────────────────────────

def _find_data_folder() -> Path:
    return Path.home() / "Desktop" / "sales-app"


def load_rules() -> dict:
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "pricing_rules.yaml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)


# ── File classification ────────────────────────────────────

def _classify_distributor(filename: str, rules: dict) -> str | None:
    """Classify distributor from filename keywords. Returns distributor key or None."""
    lower = filename.lower()
    for dist_key, dist_cfg in rules["distributors"].items():
        if any(kw in lower for kw in dist_cfg["filename_keywords"]):
            return dist_key
    return None


def _classify_distributor_from_data(df: pd.DataFrame, rules: dict) -> str | None:
    """Fallback: classify distributor from PAYER column in data."""
    payer_col = None
    for c in df.columns:
        if c.strip().upper() == "PAYER":
            payer_col = c
            break
    if not payer_col:
        return None
    first_payer = str(df[payer_col].dropna().iloc[0]).strip()
    for dist_key, dist_cfg in rules["distributors"].items():
        if first_payer in dist_cfg["payer_ids"]:
            return dist_key
    return None


def _classify_list_type(filename: str, rules: dict) -> str | None:
    """Classify list type from filename keywords. Returns list_type or None."""
    lower = filename.lower()
    # Check in priority order: location first, then end_user, then master
    for lt in ("location", "end_user", "master"):
        lt_cfg = rules["list_types"][lt]
        if any(kw in lower for kw in lt_cfg["filename_keywords"]):
            return lt
    return None


# ── File reading ───────────────────────────────────────────

def _read_file(filepath: Path) -> pd.DataFrame | None:
    ext = filepath.suffix.lower()
    try:
        if ext == ".numbers":
            return _read_numbers(filepath)
        elif ext == ".xlsx":
            return pd.read_excel(filepath, engine="openpyxl")
        elif ext in (".csv", ".tsv"):
            sep = "\t" if ext == ".tsv" else ","
            return pd.read_csv(filepath, sep=sep)
        else:
            return None
    except Exception as e:
        warnings.warn(f"Could not read {filepath.name}: {e}")
        return None


def _read_numbers(filepath: Path) -> pd.DataFrame:
    from numbers_parser import Document
    doc = Document(str(filepath))
    sheet = doc.sheets[0]
    table = sheet.tables[0]
    headers = []
    for c in range(table.num_cols):
        val = table.cell(0, c).value
        headers.append(str(val).strip() if val is not None else f"col_{c}")
    rows = []
    for r in range(1, table.num_rows):
        row = [table.cell(r, c).value for c in range(table.num_cols)]
        rows.append(row)
    return pd.DataFrame(rows, columns=headers)


def _read_uploaded_file(uploaded_file) -> pd.DataFrame | None:
    """Read a Streamlit UploadedFile into a DataFrame."""
    name = uploaded_file.name
    ext = Path(name).suffix.lower()
    try:
        if ext == ".numbers":
            # numbers-parser requires a file path, so write to temp file
            with tempfile.NamedTemporaryFile(suffix=".numbers", delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp.flush()
                return _read_numbers(Path(tmp.name))
        elif ext == ".xlsx":
            return pd.read_excel(io.BytesIO(uploaded_file.getvalue()), engine="openpyxl")
        elif ext in (".csv", ".tsv"):
            sep = "\t" if ext == ".tsv" else ","
            return pd.read_csv(io.BytesIO(uploaded_file.getvalue()), sep=sep)
        else:
            return None
    except Exception as e:
        warnings.warn(f"Could not read uploaded file {name}: {e}")
        return None


# ── Column normalization ───────────────────────────────────

def _map_columns(df: pd.DataFrame, mapping: dict[str, dict]) -> dict[str, str]:
    """
    Map normalized field names to actual column names using a two-pass strategy:
    1. Exact match (case-insensitive, stripped)
    2. Contains match (case-insensitive substring)
    Returns {normalized_name: actual_column_name}.
    """
    df_cols_stripped = {c.strip(): c for c in df.columns}
    df_cols_lower = {c.strip().lower(): c for c in df.columns}
    result = {}

    for field_name, candidates in mapping.items():
        found = False
        # Pass 1: exact match
        exact_list = candidates.get("exact", [])
        for cand in exact_list:
            cand_lower = cand.strip().lower()
            if cand_lower in df_cols_lower:
                result[field_name] = df_cols_lower[cand_lower]
                found = True
                break
        if found:
            continue
        # Pass 2: contains match
        contains_list = candidates.get("contains", [])
        for cand in contains_list:
            cand_lower = cand.strip().lower()
            for col_lower, col_orig in df_cols_lower.items():
                if cand_lower in col_lower and col_orig not in result.values():
                    result[field_name] = col_orig
                    found = True
                    break
            if found:
                break

    return result


def _safe_float(series: pd.Series) -> pd.Series:
    """Coerce a series to float, replacing invalid values with NaN."""
    return pd.to_numeric(series, errors="coerce")


def _safe_str(series: pd.Series) -> pd.Series:
    """Coerce to string, strip whitespace."""
    return series.fillna("").astype(str).str.strip()


def _normalize_master(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    if "part_number" in col_map:
        out["part_number"] = _safe_str(df[col_map["part_number"]])
    if "description" in col_map:
        out["description"] = _safe_str(df[col_map["description"]])
    if "uom" in col_map:
        out["uom"] = _safe_str(df[col_map["uom"]])
    if "package_qty" in col_map:
        out["package_qty"] = _safe_float(df[col_map["package_qty"]])
    if "weight" in col_map:
        out["weight"] = _safe_float(df[col_map["weight"]])
    if "list_price" in col_map:
        out["list_price"] = _safe_float(df[col_map["list_price"]])
    if "tier_price" in col_map:
        out["tier_price"] = _safe_float(df[col_map["tier_price"]])
    if "cmg_price" in col_map:
        out["cmg_price"] = _safe_float(df[col_map["cmg_price"]])
    if "upc" in col_map:
        out["upc"] = _safe_str(df[col_map["upc"]])
    if "product_class" in col_map:
        out["product_class"] = _safe_str(df[col_map["product_class"]])
    return out


def _normalize_special(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    if "part_number" in col_map:
        out["part_number"] = _safe_str(df[col_map["part_number"]])
    if "description" in col_map:
        out["description"] = _safe_str(df[col_map["description"]])
    if "uom" in col_map:
        out["uom"] = _safe_str(df[col_map["uom"]])
    if "price" in col_map:
        out["price"] = _safe_float(df[col_map["price"]])
    for field in ("alloy_surcharge", "tariff_surcharge"):
        if field in col_map:
            out[field] = _safe_float(df[col_map[field]]).fillna(0)
        else:
            out[field] = 0.0
    for field in ("customer_name", "end_user_name", "end_user_acct",
                   "sap_acct", "address", "city", "state"):
        if field in col_map:
            out[field] = _safe_str(df[col_map[field]])
        else:
            out[field] = ""
    return out


# ── Public API ─────────────────────────────────────────────

class PriceData:
    """Container for all loaded price data."""

    def __init__(self):
        self.master: dict[str, pd.DataFrame] = {}
        self.end_user: dict[str, pd.DataFrame] = {}
        self.location: dict[str, pd.DataFrame] = {}
        self.warnings: list[str] = []
        self.loaded_files: list[dict] = []
        self.skipped_files: list[dict] = []

    @property
    def distributor_keys(self) -> list[str]:
        keys = set()
        keys.update(self.master.keys())
        keys.update(self.end_user.keys())
        keys.update(self.location.keys())
        return sorted(keys)

    def get_end_user_names(self, distributor_key: str) -> list[str]:
        """Get distinct end-user names for a distributor."""
        eu_df = self.end_user.get(distributor_key, pd.DataFrame())
        if eu_df.empty:
            return []
        names = set()
        for col in ("end_user_name", "customer_name"):
            if col in eu_df.columns:
                vals = eu_df[col].dropna().astype(str).str.strip()
                names.update(v for v in vals if v and v != "nan")
        return sorted(names)

    def get_location_names(self, distributor_key: str) -> list[str]:
        """Get distinct location identifiers for a distributor."""
        loc_df = self.location.get(distributor_key, pd.DataFrame())
        if loc_df.empty:
            return []
        names = set()
        for _, row in loc_df.iterrows():
            parts = []
            cust = str(row.get("customer_name", "")).strip()
            city = str(row.get("city", "")).strip()
            state = str(row.get("state", "")).strip()
            if cust and cust != "nan":
                parts.append(cust)
            if city and city != "nan" and state and state != "nan":
                parts.append(f"{city}, {state}")
            elif city and city != "nan":
                parts.append(city)
            label = " -- ".join(parts)
            if label:
                names.add(label)
        return sorted(names)

    def summary(self) -> dict:
        """Return index summary statistics."""
        stats = {}
        for dist_key in self.distributor_keys:
            m = self.master.get(dist_key)
            e = self.end_user.get(dist_key)
            l = self.location.get(dist_key)
            stats[dist_key] = {
                "master_rows": len(m) if m is not None else 0,
                "end_user_rows": len(e) if e is not None else 0,
                "location_rows": len(l) if l is not None else 0,
                "files": [f for f in self.loaded_files if f["distributor_key"] == dist_key],
            }
        return stats


def _process_file(fname: str, df: pd.DataFrame, rules: dict, data: PriceData):
    """Classify, normalize, and store a single file's data into PriceData."""
    master_mapping = rules["column_mappings"]["master"]
    special_mapping = rules["column_mappings"]["special"]

    # Classify list type
    list_type = _classify_list_type(fname, rules)
    if list_type is None:
        data.skipped_files.append({"file": fname, "reason": "Unknown list type (no keyword match)"})
        data.warnings.append(f"Skipped (unknown list type): {fname}")
        return

    # Classify distributor from filename first
    dist_key_from_name = _classify_distributor(fname, rules)

    # Check if this is a universal file (applies to all distributors)
    universal_kws = rules.get("universal_keywords", [])
    is_universal = (dist_key_from_name is None and
                    any(kw in fname.lower() for kw in universal_kws))

    # Fall back to data-level classification if not universal
    dist_key = dist_key_from_name
    if dist_key is None and not is_universal:
        dist_key = _classify_distributor_from_data(df, rules)

    if dist_key is None and not is_universal:
        data.skipped_files.append({"file": fname, "reason": "Unknown distributor"})
        data.warnings.append(f"Skipped (unknown distributor): {fname}")
        return

    # Map and normalize columns
    mapping = master_mapping if list_type == "master" else special_mapping
    col_map = _map_columns(df, mapping)

    if "part_number" not in col_map or "description" not in col_map:
        data.skipped_files.append({
            "file": fname,
            "reason": f"Missing required columns. Found: {list(col_map.keys())}",
        })
        data.warnings.append(f"Skipped (missing required columns): {fname}")
        return

    # Normalize
    if list_type == "master":
        normalized = _normalize_master(df, col_map)
    else:
        normalized = _normalize_special(df, col_map)

    # Drop rows with empty part numbers
    orig_len = len(normalized)
    normalized = normalized[normalized["part_number"].str.len() > 0]
    skipped_rows = orig_len - len(normalized)

    normalized["_source_file"] = fname

    # Determine which distributors to load into
    if is_universal:
        target_dist_keys = list(rules["distributors"].keys())
    else:
        target_dist_keys = [dist_key]

    for tdk in target_dist_keys:
        target_map = {
            "master": data.master,
            "end_user": data.end_user,
            "location": data.location,
        }
        target = target_map[list_type]
        if tdk in target:
            target[tdk] = pd.concat([target[tdk], normalized], ignore_index=True)
        else:
            target[tdk] = normalized.copy()

    first_dist_name = rules["distributors"][target_dist_keys[0]]["display_name"]
    universal_note = f" (universal → {len(target_dist_keys)} distributors)" if is_universal else ""

    data.loaded_files.append({
        "file": fname,
        "distributor_key": target_dist_keys[0],
        "distributor": first_dist_name + universal_note,
        "type": list_type.replace("_", " ").title(),
        "rows": len(normalized),
        "skipped_rows": skipped_rows,
        "columns_mapped": list(col_map.keys()),
    })


def load_all(rules: dict | None = None) -> PriceData:
    """
    Scan the data folder, read all supported files, classify, normalize,
    and return a PriceData object.
    """
    if rules is None:
        rules = load_rules()

    data = PriceData()
    folder = _find_data_folder()

    if not folder.exists():
        data.warnings.append(
            f"Data folder not found: {folder}\n"
            f"Please create ~/Desktop/sales-app/ and add your price files."
        )
        return data

    supported_exts = {".numbers", ".xlsx", ".csv", ".tsv"}
    files = [f for f in folder.iterdir()
             if f.is_file() and f.suffix.lower() in supported_exts]

    if not files:
        data.warnings.append(f"No price files found in {folder}")
        return data

    for filepath in sorted(files):
        df = _read_file(filepath)
        if df is None or df.empty:
            data.skipped_files.append({"file": filepath.name, "reason": "Empty or unreadable"})
            data.warnings.append(f"Skipped (empty/unreadable): {filepath.name}")
            continue
        _process_file(filepath.name, df, rules, data)

    return data


def load_from_uploads(uploaded_files: list, rules: dict | None = None) -> PriceData:
    """
    Process Streamlit UploadedFile objects into a PriceData object.
    Same classification/normalization as load_all() but reads from uploads.
    """
    if rules is None:
        rules = load_rules()

    data = PriceData()

    if not uploaded_files:
        data.warnings.append("No files uploaded. Use the file uploader in the sidebar.")
        return data

    for uploaded_file in uploaded_files:
        fname = uploaded_file.name
        df = _read_uploaded_file(uploaded_file)
        if df is None or df.empty:
            data.skipped_files.append({"file": fname, "reason": "Empty or unreadable"})
            data.warnings.append(f"Skipped (empty/unreadable): {fname}")
            continue
        _process_file(fname, df, rules, data)

    return data
