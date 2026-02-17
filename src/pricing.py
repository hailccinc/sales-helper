"""
pricing.py — Price lookup with precedence logic.

Precedence (highest to lowest):
  1. Location-specific special pricing   (priority 80)
  2. End-user special pricing            (priority 50)
  3. Master tier price                   (priority 10)
  4. Master list price                   (priority 5)

When user selects a specific end-user or location, only that layer is applied.
If neither is selected, all available layers are shown and best price wins by precedence.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class PriceEntry:
    price: float
    source: str          # "Master List", "Master Tier", "End User", "Location Special"
    surcharges: float = 0.0
    total: float = 0.0
    context: str = ""
    source_file: str = ""

    def __post_init__(self):
        self.total = round(self.price + self.surcharges, 4)


@dataclass
class ProductPricing:
    part_number: str
    description: str
    uom: str
    package_qty: float | None = None
    weight: float | None = None
    upc: str = ""
    best_price: PriceEntry | None = None
    all_prices: list[PriceEntry] = field(default_factory=list)


def get_pricing(
    part_number: str,
    distributor_key: str,
    price_data,
    selected_end_user: str | None = None,
    selected_location: str | None = None,
) -> ProductPricing:
    """
    Look up all pricing for a part number at a distributor.

    If selected_end_user is set, only that end-user's pricing is included.
    If selected_location is set, only that location's pricing is included.
    """
    result = ProductPricing(part_number=part_number, description="", uom="")
    pn = part_number.strip()

    # 1. Master prices (always included as baseline)
    master_df = price_data.master.get(distributor_key, pd.DataFrame())
    if not master_df.empty:
        matches = master_df[master_df["part_number"] == pn]
        for _, row in matches.iterrows():
            if not result.description:
                result.description = str(row.get("description", ""))
                result.uom = str(row.get("uom", ""))
                result.package_qty = row.get("package_qty")
                result.weight = row.get("weight")
                result.upc = str(row.get("upc", ""))

            list_p = row.get("list_price")
            if pd.notna(list_p) and float(list_p) > 0:
                result.all_prices.append(PriceEntry(
                    price=float(list_p), source="Master List",
                    source_file=str(row.get("_source_file", "")),
                ))
            tier_p = row.get("tier_price")
            if pd.notna(tier_p) and float(tier_p) > 0:
                result.all_prices.append(PriceEntry(
                    price=float(tier_p), source="Master Tier",
                    source_file=str(row.get("_source_file", "")),
                ))

    # 2. End-user prices
    eu_df = price_data.end_user.get(distributor_key, pd.DataFrame())
    if not eu_df.empty:
        matches = eu_df[eu_df["part_number"] == pn]
        for _, row in matches.iterrows():
            eu_name = str(row.get("end_user_name", "")).strip()
            cust_name = str(row.get("customer_name", "")).strip()
            context = eu_name if eu_name and eu_name != "nan" else cust_name

            # If user selected a specific end-user, filter
            if selected_end_user:
                if context != selected_end_user and eu_name != selected_end_user and cust_name != selected_end_user:
                    continue

            if not result.description:
                result.description = str(row.get("description", ""))
                result.uom = str(row.get("uom", ""))
            price_val = row.get("price")
            if pd.notna(price_val) and float(price_val) > 0:
                alloy = float(row.get("alloy_surcharge", 0) or 0)
                tariff = float(row.get("tariff_surcharge", 0) or 0)
                result.all_prices.append(PriceEntry(
                    price=float(price_val), source="End User",
                    surcharges=alloy + tariff, context=context,
                    source_file=str(row.get("_source_file", "")),
                ))

    # 3. Location-specific prices
    loc_df = price_data.location.get(distributor_key, pd.DataFrame())
    if not loc_df.empty:
        matches = loc_df[loc_df["part_number"] == pn]
        for _, row in matches.iterrows():
            loc_parts = []
            cust = str(row.get("customer_name", "")).strip()
            city = str(row.get("city", "")).strip()
            state = str(row.get("state", "")).strip()
            if cust and cust != "nan":
                loc_parts.append(cust)
            if city and city != "nan" and state and state != "nan":
                loc_parts.append(f"{city}, {state}")
            elif city and city != "nan":
                loc_parts.append(city)
            context = " -- ".join(loc_parts) if loc_parts else ""

            # If user selected a specific location, filter
            if selected_location:
                if context != selected_location:
                    continue

            if not result.description:
                result.description = str(row.get("description", ""))
                result.uom = str(row.get("uom", ""))
            price_val = row.get("price")
            if pd.notna(price_val) and float(price_val) > 0:
                alloy = float(row.get("alloy_surcharge", 0) or 0)
                tariff = float(row.get("tariff_surcharge", 0) or 0)
                result.all_prices.append(PriceEntry(
                    price=float(price_val), source="Location Special",
                    surcharges=alloy + tariff, context=context,
                    source_file=str(row.get("_source_file", "")),
                ))

    # Determine best price (highest precedence)
    precedence = {"Location Special": 1, "End User": 2, "Master Tier": 3, "Master List": 4}
    if result.all_prices:
        result.all_prices.sort(key=lambda p: (precedence.get(p.source, 99), p.total))
        result.best_price = result.all_prices[0]

    return result


def find_variants(part_number: str, description: str, distributor_key: str, price_data) -> list[dict]:
    """Find packaging/size variants of a product (same product line, different packaging)."""
    master_df = price_data.master.get(distributor_key, pd.DataFrame())
    if master_df.empty:
        return []

    pn = part_number.strip()
    prefix = pn[:min(6, len(pn))]

    # Also match by description identity (first 4 tokens)
    desc_upper = description.upper()
    desc_tokens = desc_upper.split()
    identity = " ".join(desc_tokens[:4]) if len(desc_tokens) >= 4 else desc_upper

    candidates = master_df[
        (master_df["part_number"].str.startswith(prefix, na=False)) |
        (master_df["description"].str.upper().str.contains(identity, na=False, regex=False))
    ].copy()

    candidates = candidates[candidates["part_number"] != pn]

    variants = []
    for _, row in candidates.head(15).iterrows():
        tier_p = row.get("tier_price")
        list_p = row.get("list_price")
        price = float(tier_p) if pd.notna(tier_p) and float(tier_p) > 0 else (
            float(list_p) if pd.notna(list_p) and float(list_p) > 0 else None
        )
        variants.append({
            "part_number": str(row["part_number"]),
            "description": str(row["description"]),
            "uom": str(row.get("uom", "")),
            "package_qty": row.get("package_qty"),
            "price": price,
        })

    return variants
