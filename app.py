"""
Sales Helper — Product lookup & pricing tool.

Run with:  streamlit run app.py
"""

import math
from datetime import datetime

import pandas as pd
import streamlit as st
import yaml
from pathlib import Path

from src.loader import load_all, load_from_uploads, load_rules, PriceData
from src.search import search_products, parse_query
from src.pricing import get_pricing, find_variants
from src.inventory import check_availability
from src.enrichment import enrich_dataframe, load_cache, get_enriched_description


# ── Config ─────────────────────────────────────────────────

RULES_PATH = Path(__file__).parent / "config" / "pricing_rules.yaml"


@st.cache_data
def cached_load_rules():
    with open(RULES_PATH) as f:
        return yaml.safe_load(f)


def load_price_data(rules) -> PriceData:
    return load_all(rules)


# ── Quote basket helpers ─────────────────────────────────────

def _init_basket():
    if "basket" not in st.session_state:
        st.session_state.basket = []


def _add_to_basket(part_number: str, description: str, uom: str, unit_price: float, source: str, source_file: str):
    _init_basket()
    # Check if already in basket
    for item in st.session_state.basket:
        if item["part_number"] == part_number:
            item["qty"] += 1
            return
    st.session_state.basket.append({
        "part_number": part_number,
        "description": description,
        "uom": uom,
        "qty": 1,
        "unit_price": unit_price,
        "source": source,
        "source_file": source_file,
    })


def _remove_from_basket(part_number: str):
    st.session_state.basket = [i for i in st.session_state.basket if i["part_number"] != part_number]


def _export_markdown(basket: list[dict], distributor: str) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# Price Quote - {distributor}",
        f"**Date:** {now}",
        "",
        "| Part Number | Description | UOM | Qty | Unit Price | Extended |",
        "|---|---|---|---:|---:|---:|",
    ]
    total = 0
    for item in basket:
        ext = item["qty"] * item["unit_price"]
        total += ext
        lines.append(
            f"| {item['part_number']} | {item['description'][:50]} | {item['uom']} "
            f"| {item['qty']} | ${item['unit_price']:.2f} | ${ext:.2f} |"
        )
    lines.append(f"| | | | | **Total:** | **${total:.2f}** |")
    lines.append("")
    sources = set(item["source"] for item in basket)
    lines.append(f"*Pricing source: {', '.join(sources)}*")
    lines.append("")
    lines.append("---")
    lines.append("*Prices are subject to change without notice. This quote is for informational purposes only and does not constitute a binding offer.*")
    return "\n".join(lines)


def _export_csv(basket: list[dict], distributor: str) -> str:
    lines = [
        f"# Price Quote - {distributor} - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "Part Number,Description,UOM,Qty,Unit Price,Extended,Source",
    ]
    for item in basket:
        ext = item["qty"] * item["unit_price"]
        desc = item["description"].replace(",", " ")
        lines.append(
            f"{item['part_number']},{desc},{item['uom']},"
            f"{item['qty']},{item['unit_price']:.2f},{ext:.2f},{item['source']}"
        )
    total = sum(i["qty"] * i["unit_price"] for i in basket)
    lines.append(f",,,,Total,{total:.2f},")
    lines.append("")
    lines.append("# Prices subject to change without notice. Informational purposes only.")
    return "\n".join(lines)


def _export_html(basket: list[dict], distributor: str) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    sources = set(item["source"] for item in basket)
    rows_html = ""
    total = 0
    for item in basket:
        ext = item["qty"] * item["unit_price"]
        total += ext
        rows_html += f"""
        <tr>
            <td>{item['part_number']}</td>
            <td>{item['description'][:60]}</td>
            <td>{item['uom']}</td>
            <td style="text-align:right">{item['qty']}</td>
            <td style="text-align:right">${item['unit_price']:.2f}</td>
            <td style="text-align:right">${ext:.2f}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head>
<style>
    body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 40px auto; }}
    h1 {{ color: #333; border-bottom: 2px solid #0066cc; padding-bottom: 10px; }}
    table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
    th {{ background: #0066cc; color: white; padding: 8px; text-align: left; }}
    td {{ padding: 6px 8px; border-bottom: 1px solid #ddd; }}
    tr:nth-child(even) {{ background: #f9f9f9; }}
    .total {{ font-weight: bold; font-size: 1.1em; }}
    .meta {{ color: #666; font-size: 0.9em; }}
    .disclaimer {{ margin-top: 30px; padding: 10px; background: #f5f5f5; font-size: 0.8em; color: #888; }}
    @media print {{ body {{ margin: 10px; }} }}
</style>
</head>
<body>
<h1>Price Quote - {distributor}</h1>
<p class="meta">Date: {now} | Source: {', '.join(sources)}</p>

<table>
<thead>
<tr><th>Part Number</th><th>Description</th><th>UOM</th><th>Qty</th><th>Unit Price</th><th>Extended</th></tr>
</thead>
<tbody>
{rows_html}
<tr class="total">
    <td colspan="5" style="text-align:right">Total:</td>
    <td style="text-align:right">${total:.2f}</td>
</tr>
</tbody>
</table>

<div class="disclaimer">
Prices are subject to change without notice. This quote is for informational purposes only
and does not constitute a binding offer. Contact your sales representative for a formal quotation.
</div>
</body>
</html>"""


# ── Product detail renderer ───────────────────────────────

def show_product_detail(
    part_number: str,
    description: str,
    distributor_key: str,
    price_data: PriceData,
    rules: dict,
    enrichment_cache: dict,
    selected_end_user: str | None = None,
    selected_location: str | None = None,
    inventory_csv_path: str = "",
):
    """Render full product detail inside an expander."""
    pricing = get_pricing(part_number, distributor_key, price_data,
                          selected_end_user=selected_end_user,
                          selected_location=selected_location)
    inv_df = st.session_state.get("_inventory_df")
    avail = check_availability(part_number, distributor_key, csv_path=inventory_csv_path,
                               inventory_df=inv_df)
    dist_name = rules["distributors"][distributor_key]["display_name"]

    # Enriched description
    enriched = get_enriched_description(part_number, pricing.description or description, enrichment_cache)

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(f"**Part Number:** `{part_number}`")
        st.markdown(f"**Raw:** {pricing.description or description}")
        if enriched and enriched != (pricing.description or description):
            st.markdown(f"**Decoded:** {enriched}")
        if pricing.uom:
            st.markdown(f"**UOM:** {pricing.uom}")
        try:
            if pricing.package_qty and not math.isnan(pricing.package_qty) and pricing.package_qty > 0:
                st.markdown(f"**Standard Package:** {pricing.package_qty:g}")
        except (ValueError, TypeError):
            pass
        if pricing.upc and pricing.upc not in ("nan", ""):
            st.markdown(f"**UPC:** {pricing.upc}")

    with col2:
        if pricing.best_price:
            bp = pricing.best_price
            if bp.surcharges > 0:
                st.metric(
                    label=f"Best Price ({bp.source})",
                    value=f"${bp.total:.2f}/{pricing.uom}",
                    help=f"Base: ${bp.price:.2f} + surcharges: ${bp.surcharges:.2f}",
                )
            else:
                st.metric(
                    label=f"Best Price ({bp.source})",
                    value=f"${bp.price:.2f}/{pricing.uom}",
                )
            if bp.source_file:
                st.caption(f"Source: {bp.source_file}")
            if bp.context:
                st.caption(bp.context)

            # Add to Quote button
            if st.button(f"Add to Quote", key=f"add_{part_number}"):
                _add_to_basket(
                    part_number=part_number,
                    description=enriched if (enriched and enriched != description) else (pricing.description or description),
                    uom=pricing.uom,
                    unit_price=bp.total if bp.surcharges > 0 else bp.price,
                    source=bp.source,
                    source_file=bp.source_file or "",
                )
                st.toast(f"Added {part_number} to quote basket")

        if avail["status"] == "not_configured":
            st.caption("Availability: not configured")
        elif avail["status"] == "error":
            st.error(f"Inventory: {avail['message']}")
        elif avail["status"] == "not_found":
            st.caption("Not in inventory file")
        elif avail["status"] == "in_stock":
            st.success(f"In stock: {avail['message']}")
        elif avail["status"] == "out_of_stock":
            st.error("Out of stock")

        # Show inventory detail rows if available
        if avail.get("rows"):
            inv_rows = avail["rows"]
            if len(inv_rows) > 1 or any(r.get("location") for r in inv_rows):
                with st.container():
                    for r in inv_rows:
                        loc = r.get("location", "")
                        oh = r.get("on_hand", 0)
                        bo = r.get("backorder", 0)
                        eta = r.get("eta", "")
                        parts = [f"On Hand: {oh:g}"]
                        if bo:
                            parts.append(f"Backorder: {bo:g}")
                        if eta:
                            parts.append(f"ETA: {eta}")
                        if loc:
                            parts.append(f"Location: {loc}")
                        st.caption(" | ".join(parts))

    # All pricing tiers
    if pricing.all_prices:
        st.markdown("---")
        st.markdown(f"**All Pricing for {dist_name}:**")
        price_rows = []
        for p in pricing.all_prices:
            price_rows.append({
                "Source": p.source,
                "Base Price": f"${p.price:.4f}",
                "Surcharges": f"${p.surcharges:.2f}" if p.surcharges > 0 else "---",
                "Total": f"${p.total:.4f}" if p.surcharges > 0 else f"${p.price:.4f}",
                "File": p.source_file or "---",
                "Context": p.context or "---",
            })
        st.dataframe(price_rows, use_container_width=True, hide_index=True)

    # Variants — selectable
    variants = find_variants(part_number, description, distributor_key, price_data)
    if variants:
        st.markdown("---")
        st.markdown("**Packaging Variants:**")

        var_rows = []
        for v in variants:
            pkg_qty = v.get("package_qty")
            try:
                pkg = f"{pkg_qty:g}" if pkg_qty and not math.isnan(float(pkg_qty)) and float(pkg_qty) > 0 else "---"
            except (ValueError, TypeError):
                pkg = "---"
            price_str = f"${v['price']:.2f}" if v["price"] else "---"
            var_enriched = get_enriched_description(v["part_number"], v["description"], enrichment_cache)
            var_rows.append({
                "Part Number": v["part_number"],
                "Description": v["description"],
                "Decoded": var_enriched if var_enriched != v["description"] else "---",
                "UOM": v["uom"],
                "Pkg Qty": pkg,
                "Tier Price": price_str,
            })

        st.dataframe(var_rows, use_container_width=True, hide_index=True)


# ── Page setup ─────────────────────────────────────────────

st.set_page_config(page_title="Sales Helper", page_icon="S", layout="wide")

_init_basket()
rules = cached_load_rules()

# Load enrichment cache (always available — ships with the repo)
if "enrichment_cache" not in st.session_state:
    st.session_state.enrichment_cache = load_cache()
enrichment_cache: dict = st.session_state.enrichment_cache


def _load_and_enrich(pd_data: PriceData):
    """Enrich master DataFrames and store in session state."""
    for dist_key, master_df in pd_data.master.items():
        if "enriched_description" not in master_df.columns:
            master_df["enriched_description"] = enrich_dataframe(master_df, enrichment_cache)
    st.session_state.price_data = pd_data
    st.session_state.enriched = True


# ── Sidebar ────────────────────────────────────────────────

with st.sidebar:
    st.title("Sales Helper")

    # File upload section
    with st.expander("Upload Price Files", expanded="price_data" not in st.session_state):
        uploaded_files = st.file_uploader(
            "Drop your price files here",
            type=["numbers", "xlsx", "csv", "tsv"],
            accept_multiple_files=True,
            help="Upload .numbers, .xlsx, or .csv price files from any device",
        )

        if uploaded_files:
            # Build a key from filenames + sizes to detect changes
            upload_key = tuple((f.name, f.size) for f in uploaded_files)
            if st.session_state.get("_upload_key") != upload_key:
                with st.spinner("Processing uploaded files..."):
                    pd_data = load_from_uploads(uploaded_files, rules)
                    _load_and_enrich(pd_data)
                    st.session_state._upload_key = upload_key
                st.rerun()

    # Auto-load from local folder if no uploads and no data yet
    if "price_data" not in st.session_state:
        pd_data = load_price_data(rules)
        _load_and_enrich(pd_data)

    price_data: PriceData = st.session_state.price_data

    # Distributor selector
    dist_options = {}
    for key in rules["distributors"]:
        dist_options[rules["distributors"][key]["display_name"]] = key

    available = {name: key for name, key in dist_options.items()
                 if key in price_data.master or key in price_data.end_user
                 or key in price_data.location}
    if not available:
        available = dist_options

    selected_dist_name = st.selectbox("Distributor", options=list(available.keys()))
    selected_dist_key = available[selected_dist_name]

    # End-user dropdown (optional)
    eu_names = price_data.get_end_user_names(selected_dist_key)
    selected_end_user = None
    if eu_names:
        eu_options = ["(All / None)"] + eu_names
        eu_choice = st.selectbox("End User (optional)", options=eu_options)
        if eu_choice != "(All / None)":
            selected_end_user = eu_choice

    # Location dropdown (optional)
    loc_names = price_data.get_location_names(selected_dist_key)
    selected_location = None
    if loc_names:
        loc_options = ["(All / None)"] + loc_names
        loc_choice = st.selectbox("Location (optional)", options=loc_options)
        if loc_choice != "(All / None)":
            selected_location = loc_choice

    # Inventory CSV upload
    with st.expander("Inventory File (optional)"):
        inv_upload = st.file_uploader(
            "Upload inventory export",
            type=["csv", "xlsx", "tsv"],
            help="Export from Power BI or your ERP, then upload here. Needs part_number + on_hand columns.",
            key="inv_upload",
        )
        if inv_upload:
            import io as _io
            from src.inventory import _normalize_inv_columns
            try:
                if inv_upload.name.endswith(".csv"):
                    inv_df = pd.read_csv(_io.BytesIO(inv_upload.getvalue()))
                elif inv_upload.name.endswith(".xlsx"):
                    inv_df = pd.read_excel(_io.BytesIO(inv_upload.getvalue()), engine="openpyxl")
                else:
                    inv_df = pd.read_csv(_io.BytesIO(inv_upload.getvalue()), sep="\t")
                normalized_inv = _normalize_inv_columns(inv_df)
                if "part_number" in normalized_inv.columns and "on_hand" in normalized_inv.columns:
                    st.session_state._inventory_df = normalized_inv
                    st.caption(f"Loaded {len(normalized_inv)} inventory rows")
                else:
                    st.error(f"Missing required columns. Found: {list(inv_df.columns)}")
            except Exception as e:
                st.error(f"Error: {e}")
    inv_path = ""

    st.divider()

    # Reload
    if st.button("Reload Price Files", use_container_width=True):
        for k in ("price_data", "enrichment_cache", "enriched", "_upload_key"):
            st.session_state.pop(k, None)
        st.rerun()

    # Index Summary
    with st.expander("Index Summary", expanded=False):
        summary = price_data.summary()
        for dist_key, stats in summary.items():
            dist_name = rules["distributors"].get(dist_key, {}).get("display_name", dist_key)
            st.markdown(f"**{dist_name}**")
            c1, c2, c3 = st.columns(3)
            c1.metric("Master", f"{stats['master_rows']:,}")
            c2.metric("EUP", f"{stats['end_user_rows']:,}")
            c3.metric("Loc.", f"{stats['location_rows']:,}")

            for f in stats["files"]:
                skip_note = f" ({f['skipped_rows']} rows skipped)" if f.get("skipped_rows", 0) > 0 else ""
                st.caption(f"{f['file']}  \n{f['type']} | {f['rows']:,} rows{skip_note}")

        # Enrichment stats
        st.divider()
        st.markdown("**Description Enrichment**")
        st.caption(f"Cache entries: {len(enrichment_cache):,}")
        master_df = price_data.master.get(selected_dist_key)
        if master_df is not None and "enriched_description" in master_df.columns:
            enriched_count = (master_df["enriched_description"] != master_df["description"]).sum()
            st.caption(f"Enriched: {enriched_count:,} / {len(master_df):,}")

    # Loaded files detail
    if price_data.loaded_files:
        with st.expander("Loaded Files Detail", expanded=False):
            for f in price_data.loaded_files:
                cols_str = ", ".join(f.get("columns_mapped", []))
                st.caption(
                    f"**{f['file']}**  \n"
                    f"{f['distributor']} | {f['type']} | {f['rows']:,} rows  \n"
                    f"Columns: {cols_str}"
                )

    # Warnings / skipped
    if price_data.warnings or price_data.skipped_files:
        with st.expander("Warnings & Skipped Files", expanded=False):
            for w in price_data.warnings:
                st.warning(w)
            for s in price_data.skipped_files:
                st.caption(f"**{s['file']}**: {s['reason']}")


# ── Main content: Search ───────────────────────────────────

search_tab, basket_tab, inventory_tab = st.tabs(["Search", f"Quote Basket ({len(st.session_state.basket)})", "Inventory"])

with search_tab:
    query = st.text_input(
        "Search products",
        placeholder='e.g. "s6 wire 0.045", "70s-6 33 lb", "tig rod 308l 1/16", "cutmaster 80 electrode"',
        label_visibility="collapsed",
    )

    master_df = price_data.master.get(selected_dist_key)

    if query and master_df is not None and not master_df.empty:
        results = search_products(
            query, master_df,
            max_results=12,
            min_score=30,
        )

        if results.empty:
            st.info("No matches found. Try different keywords.")
        else:
            # Show parsed query tokens for transparency
            pq = parse_query(query)
            token_parts = []
            if pq.diameters:
                token_parts.append(f"diameter: {', '.join(pq.diameters)}")
            if pq.alloys:
                token_parts.append(f"alloy: {', '.join(pq.alloys)}")
            if pq.pkg_weights:
                token_parts.append(f"pkg: {', '.join(w + '#' for w in pq.pkg_weights)}")
            if pq.pkg_types:
                token_parts.append(f"type: {', '.join(pq.pkg_types)}")
            parsed_info = f" | Parsed: {' | '.join(token_parts)}" if token_parts else ""

            st.caption(f"{len(results)} matches for **{query}**{parsed_info}")

            for idx, row in results.iterrows():
                score = row["match_score"]
                pn = row["part_number"]
                desc = row["description"]
                enriched = row.get("enriched_description", "")
                uom = row.get("uom", "")
                pkg = row.get("package_qty")
                tier = row.get("tier_price")
                list_p = row.get("list_price")

                # Price display
                display_price = ""
                try:
                    if tier and not math.isnan(float(tier)) and float(tier) > 0:
                        display_price = f"${float(tier):.2f}/{uom}"
                    elif list_p and not math.isnan(float(list_p)) and float(list_p) > 0:
                        display_price = f"${float(list_p):.2f}/{uom}"
                except (ValueError, TypeError):
                    pass

                pkg_str = ""
                try:
                    if pkg and not math.isnan(float(pkg)) and float(pkg) > 0:
                        pkg_val = float(pkg)
                        pkg_str = f" | {int(pkg_val)}pk" if pkg_val == int(pkg_val) else f" | {pkg_val}pk"
                except (ValueError, TypeError):
                    pass

                # Show enriched in the expander header if different from raw
                display_desc = enriched if (enriched and enriched != desc) else desc

                # Confidence bar color
                if score >= 80:
                    score_color = "green"
                elif score >= 60:
                    score_color = "orange"
                else:
                    score_color = "red"

                with st.expander(
                    f"**{pn}** -- {display_desc}  |  "
                    f"{display_price}{pkg_str}  |  "
                    f":{score_color}[{score:.0f}%]"
                ):
                    show_product_detail(pn, desc, selected_dist_key, price_data, rules, enrichment_cache,
                                        selected_end_user=selected_end_user, selected_location=selected_location,
                                        inventory_csv_path=inv_path)

    elif query and (master_df is None or master_df.empty):
        st.warning(f"No master price data loaded for {selected_dist_name}.")

    elif not query:
        st.markdown(
            """
            ### How to use

            1. **Upload price files** using the sidebar uploader (or place files in ~/Desktop/sales-app/ locally)
            2. **Select a distributor** in the sidebar
            3. **Type a product description** in the search bar above
            4. Click a result to see full pricing details
            5. Click **Add to Quote** to build a price list

            **Search tips:**
            - Wire by size: `s6 wire 0.045`, `70s-6 33 lb`, `308l 1/16`
            - Electrodes: `7018 1/8`, `sureweld 6010 3/32`
            - TIG rod: `tig rod 308l 1/16`
            - Flux-core: `dual shield 7100 045`
            - Part numbers: `321M112200`
            - Hardgoods: `contact tip 035`, `nozzle heavy duty`
            - Machines: `rebel 285`, `cutmaster 80`
            - European brands: `oks 48.00`, `ok autrod`
            """
        )


# ── Quote Basket tab ───────────────────────────────────────

with basket_tab:
    basket = st.session_state.basket

    if not basket:
        st.info("Your quote basket is empty. Search for products and click **Add to Quote** to get started.")
    else:
        st.markdown(f"### Quote for {selected_dist_name}")

        # Editable basket table
        grand_total = 0
        items_to_remove = []

        for i, item in enumerate(basket):
            c1, c2, c3, c4, c5, c6 = st.columns([2, 3, 1, 1, 1, 1])
            with c1:
                st.text(item["part_number"])
            with c2:
                st.text(item["description"][:45])
            with c3:
                new_qty = st.number_input(
                    "Qty", value=item["qty"], min_value=1, step=1,
                    key=f"qty_{item['part_number']}_{i}",
                    label_visibility="collapsed",
                )
                basket[i]["qty"] = new_qty
            with c4:
                st.text(f"${item['unit_price']:.2f}")
            with c5:
                ext = new_qty * item["unit_price"]
                grand_total += ext
                st.text(f"${ext:.2f}")
            with c6:
                if st.button("X", key=f"rm_{item['part_number']}_{i}"):
                    items_to_remove.append(item["part_number"])

        # Process removals
        if items_to_remove:
            for pn in items_to_remove:
                _remove_from_basket(pn)
            st.rerun()

        st.markdown(f"**Total: ${grand_total:.2f}**")

        st.divider()

        # Export buttons
        col_md, col_csv, col_html, col_clear = st.columns(4)

        with col_md:
            md_content = _export_markdown(basket, selected_dist_name)
            st.download_button(
                "Export Markdown",
                data=md_content,
                file_name=f"quote_{selected_dist_key}_{datetime.now().strftime('%Y%m%d')}.md",
                mime="text/markdown",
                use_container_width=True,
            )

        with col_csv:
            csv_content = _export_csv(basket, selected_dist_name)
            st.download_button(
                "Export CSV",
                data=csv_content,
                file_name=f"quote_{selected_dist_key}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with col_html:
            html_content = _export_html(basket, selected_dist_name)
            st.download_button(
                "Export HTML",
                data=html_content,
                file_name=f"quote_{selected_dist_key}_{datetime.now().strftime('%Y%m%d')}.html",
                mime="text/html",
                use_container_width=True,
            )

        with col_clear:
            if st.button("Clear Basket", use_container_width=True, type="secondary"):
                st.session_state.basket = []
                st.rerun()


# ── Inventory tab (Power BI embed) ────────────────────────

POWERBI_REPORT_URL = (
    "https://app.powerbi.com/reportEmbed"
    "?reportId=0c44a616-6a10-4ce2-bb6a-65e60c75a5bd"
    "&autoAuth=true"
    "&ctid=ba47116b-6e71-4c27-89e4-3b4ad1994f4a"
    "&pageName=8e8690c0c5490ca1b902"
    "&bookmarkGuid=117cd99c-c02f-4c4f-9221-67de10894248"
)

POWERBI_DIRECT_URL = (
    "https://app.powerbi.com/groups/me/reports/0c44a616-6a10-4ce2-bb6a-65e60c75a5bd"
    "/8e8690c0c5490ca1b902"
    "?bookmarkGuid=117cd99c-c02f-4c4f-9221-67de10894248"
    "&ctid=ba47116b-6e71-4c27-89e4-3b4ad1994f4a"
)

with inventory_tab:
    st.markdown(f"[Open in Power BI]({POWERBI_DIRECT_URL})")
    st.components.v1.iframe(POWERBI_REPORT_URL, height=700, scrolling=True)
