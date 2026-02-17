"""
inventory.py — Inventory availability lookup.

Supports two modes:
1. CSV: Read from a local CSV file with part_number + on_hand columns
2. Power BI REST: Query a Power BI dataset (placeholder, requires setup)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml


_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "inventory_source.yaml"
_inventory_df: pd.DataFrame | None = None
_inventory_path: str = ""


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    return {}


def _normalize_inv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize inventory CSV columns to standard names."""
    col_lower = {c.strip().lower(): c for c in df.columns}
    mapping = {}

    # Part number
    for candidate in ["part_number", "part number", "partnumber", "material", "item", "sku"]:
        if candidate in col_lower:
            mapping["part_number"] = col_lower[candidate]
            break

    # On hand
    for candidate in ["on_hand", "onhand", "qty_on_hand", "available", "qty", "quantity", "in_stock"]:
        if candidate in col_lower:
            mapping["on_hand"] = col_lower[candidate]
            break

    # Backorder (optional)
    for candidate in ["backorder", "back_order", "bo", "backordered"]:
        if candidate in col_lower:
            mapping["backorder"] = col_lower[candidate]
            break

    # ETA (optional)
    for candidate in ["eta", "expected", "arrival", "due_date"]:
        if candidate in col_lower:
            mapping["eta"] = col_lower[candidate]
            break

    # Location (optional)
    for candidate in ["location", "warehouse", "branch", "site"]:
        if candidate in col_lower:
            mapping["location"] = col_lower[candidate]
            break

    out = pd.DataFrame()
    if "part_number" in mapping:
        out["part_number"] = df[mapping["part_number"]].fillna("").astype(str).str.strip()
    if "on_hand" in mapping:
        out["on_hand"] = pd.to_numeric(df[mapping["on_hand"]], errors="coerce").fillna(0)
    if "backorder" in mapping:
        out["backorder"] = pd.to_numeric(df[mapping["backorder"]], errors="coerce").fillna(0)
    if "eta" in mapping:
        out["eta"] = df[mapping["eta"]].fillna("").astype(str).str.strip()
    if "location" in mapping:
        out["location"] = df[mapping["location"]].fillna("").astype(str).str.strip()

    return out


def load_inventory_csv(csv_path: str) -> tuple[pd.DataFrame | None, str]:
    """Load and normalize an inventory CSV. Returns (df, error_message)."""
    global _inventory_df, _inventory_path

    if not csv_path:
        return None, "No inventory CSV path configured"

    path = Path(csv_path).expanduser()
    if not path.exists():
        return None, f"File not found: {path}"

    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        elif path.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(path)
        elif path.suffix.lower() == ".tsv":
            df = pd.read_csv(path, sep="\t")
        else:
            return None, f"Unsupported format: {path.suffix}"

        normalized = _normalize_inv_columns(df)
        if "part_number" not in normalized.columns or "on_hand" not in normalized.columns:
            return None, f"Missing required columns (part_number, on_hand). Found: {list(df.columns)}"

        _inventory_df = normalized
        _inventory_path = str(path)
        return normalized, ""

    except Exception as e:
        return None, f"Error reading {path.name}: {e}"


def check_availability(part_number: str, distributor: str = "", csv_path: str = "") -> dict:
    """
    Check inventory availability for a part number.

    Returns dict with keys: part_number, status, rows, message
    Each row has: on_hand, backorder, eta, location
    """
    global _inventory_df, _inventory_path

    config = _load_config()
    mode = config.get("mode", "csv")

    if mode == "powerbi":
        return {
            "part_number": part_number,
            "status": "not_configured",
            "message": "Power BI not configured. See docs/powerbi_rest.md for setup.",
            "rows": [],
        }

    # CSV mode
    # Try to load if not loaded or path changed
    target_path = csv_path or config.get("csv", {}).get("path", "")
    if not target_path:
        return {
            "part_number": part_number,
            "status": "not_configured",
            "message": "No inventory CSV configured. Set path in sidebar or config/inventory_source.yaml.",
            "rows": [],
        }

    if _inventory_df is None or _inventory_path != str(Path(target_path).expanduser()):
        df, err = load_inventory_csv(target_path)
        if err:
            return {
                "part_number": part_number,
                "status": "error",
                "message": err,
                "rows": [],
            }

    if _inventory_df is None:
        return {
            "part_number": part_number,
            "status": "not_configured",
            "message": "Inventory data not loaded",
            "rows": [],
        }

    pn = part_number.strip()
    matches = _inventory_df[_inventory_df["part_number"] == pn]

    if matches.empty:
        return {
            "part_number": part_number,
            "status": "not_found",
            "message": "Part not in inventory file",
            "rows": [],
        }

    rows = []
    total_on_hand = 0
    for _, row in matches.iterrows():
        on_hand = row.get("on_hand", 0) or 0
        total_on_hand += on_hand
        rows.append({
            "on_hand": on_hand,
            "backorder": row.get("backorder", 0) or 0,
            "eta": str(row.get("eta", "")) if "eta" in row.index else "",
            "location": str(row.get("location", "")) if "location" in row.index else "",
        })

    status = "in_stock" if total_on_hand > 0 else "out_of_stock"
    return {
        "part_number": part_number,
        "status": status,
        "message": f"{total_on_hand:g} on hand" if total_on_hand > 0 else "Out of stock",
        "rows": rows,
        "total_on_hand": total_on_hand,
    }
