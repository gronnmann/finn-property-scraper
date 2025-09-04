from __future__ import annotations

from typing import Iterable, List, Dict, Any
import pandas as pd

# ---- Selectors --------------------------------------------------------------

# Categorical (kept as strings/categories)
_CATEGORICAL_COLS = [
    "category",
    "property_type",
    "ownership_type",
    "energy_label",
    "status",
]

# Quantitative (numeric) fields
_NUMERIC_COLS = [
    "asking_price",
    "total_price",
    "transaction_costs",
    "communal_fees",
    "assessed_wealth_value",
    "shared_debt",
    "shared_equity",
    "bedrooms",
    "rooms",
    "floor",
    "year_built",
    "area_bra_i",
    "area_bra",
    "plot_area",
]

# Address columns we keep after flattening
_ADDRESS_COLS = ["address", "neighbourhood"]

# Order of columns in the exported CSV
_EXPORT_COL_ORDER = _ADDRESS_COLS + _CATEGORICAL_COLS + _NUMERIC_COLS


# ---- Core flattening --------------------------------------------------------

def _flatten_property(p: "Property") -> Dict[str, Any]:
    """Flatten a Property into a CSV-friendly dict."""
    addr_line = None
    neighbourhood = None

    if p.address is not None:
        # Only keep the string address and neighbourhood as requested
        addr_line = p.address.line
        neighbourhood = p.address.neighbourhood

    row: Dict[str, Any] = {
        "address": addr_line,
        "neighbourhood": neighbourhood,
    }

    # Categorical
    for k in _CATEGORICAL_COLS:
        row[k] = getattr(p, k, None)

    # Numeric
    for k in _NUMERIC_COLS:
        row[k] = getattr(p, k, None)

    return row


def properties_to_dataframe(properties: Iterable["Property"]) -> pd.DataFrame:
    """
    Convert a list/iterable of Property objects into a pandas DataFrame
    containing only analysis-friendly columns.
    """
    rows = [_flatten_property(p) for p in properties]
    df = pd.DataFrame(rows)

    # Ensure presence & order of columns
    for col in _EXPORT_COL_ORDER:
        if col not in df.columns:
            df[col] = pd.Series(dtype="float64" if col in _NUMERIC_COLS else "object")
    df = df[_EXPORT_COL_ORDER]

    # Coerce numerics; keep categoricals as dtype 'category' for analysis ergonomics
    if not df.empty:
        for col in _NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in _CATEGORICAL_COLS + _ADDRESS_COLS:
            df[col] = df[col].astype("category")

    return df


def properties_to_csv(properties: Iterable["Property"], path: str) -> str:
    """
    Write the flattened data to CSV at `path`. Returns the written path.
    """
    df = properties_to_dataframe(properties)
    df.to_csv(path, index=False)
    return path


# ---- Optional: pure-stdlib writer (no pandas) -------------------------------
# If you want to avoid pandas, uncomment below.
#
# import csv
# def properties_to_csv_stdlib(properties: Iterable["Property"], path: str) -> str:
#     with open(path, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=_EXPORT_COL_ORDER)
#         writer.writeheader()
#         for p in properties:
#             writer.writerow(_flatten_property(p))
#     return path


# ---- Example usage ----------------------------------------------------------
# props: List[Property] = [...]
# df = properties_to_dataframe(props)          # do analysis in Python
# properties_to_csv(props, "properties.csv")   # export for external analysis
