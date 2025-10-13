#!/usr/bin/env python3
# coding: utf-8
"""
Combine all GHCN-Daily station CSVs (US only, excluding CoCoRaHS)
into one large file for a specified region (Arizona + buffer),
date range (2010–2024), with proper PRCP conversion,
Measurement Flag filtering, and parallel reading.

Author: Omid Zandi
"""

import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# ====================== CONFIG ======================
FOLDER = Path("/ra1/pubdat/ghcn_daily/us_stations")
OUT_CSV = "ghcn_precip_2010_2024_AZ_buffer_no_CoCoRaHS.csv"

# Geographic bounding box (Arizona + buffer)
MIN_LAT, MAX_LAT = 30, 38
MIN_LON, MAX_LON = -116, -107

# Time range
START_DATE = "2010-01-01"
END_DATE   = "2024-12-31"

# Parallel settings
MAX_WORKERS = 24

# Columns and dtypes
USECOLS = [
    "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION",
    "NAME", "PRCP", "PRCP_ATTRIBUTES"
]
DTYPES = {
    "STATION": "category",
    "LATITUDE": "float32",
    "LONGITUDE": "float32",
    "ELEVATION": "float32",
    "NAME": "category",
    "PRCP": "string",
    "PRCP_ATTRIBUTES": "string",
}
# ====================================================


def filter_by_mflag(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only acceptable Measurement Flags for precipitation."""
    if "PRCP_ATTRIBUTES" not in df:
        return df

    flags = df["PRCP_ATTRIBUTES"].fillna("").astype(str).str.split(",", expand=True)
    df["MFLAG"] = flags[0].fillna("").str.strip()
    df["QFLAG"] = flags[1].fillna("").str.strip()
    df["SFLAG"] = flags[2].fillna("").str.strip()

    # Keep only high-quality measurement flags
    keep_flags = ["", "G", "B", "N", "S", "T"]
    df = df[df["MFLAG"].isin(keep_flags)]

    # Handle trace codes (S/T) → 0.0 mm
    df.loc[df["MFLAG"].isin(["S", "T"]), "PRCP"] = 0.0
    return df


def process_one_file(file: Path):
    """Read, clean, and filter a single station CSV."""
    try:
        # Skip CoCoRaHS files (US1 prefix)
        if file.name.startswith("US1"):
            return None

        df = pd.read_csv(file, usecols=USECOLS, dtype=DTYPES, parse_dates=["DATE"])
        lat, lon = df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0]

        # Geographic filtering
        if not (MIN_LAT <= lat <= MAX_LAT and MIN_LON <= lon <= MAX_LON):
            return None

        # Date filtering
        df = df[(df["DATE"] >= START_DATE) & (df["DATE"] <= END_DATE)]
        if df.empty:
            return None

        # Convert PRCP to mm (tenths of mm → mm)
        df["PRCP"] = pd.to_numeric(df["PRCP"].astype(str).str.strip(), errors="coerce") / 10.0

        # Apply MFLAG filtering
        df = filter_by_mflag(df)
        if df.empty:
            return None

        return df

    except Exception as e:
        print(f"Skipping {file.name}: {e}")
        return None


def main():
    files = sorted(FOLDER.glob("US*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {FOLDER}")

    print(f"🧩 Found {len(files)} station CSVs. Excluding CoCoRaHS (US1...).")
    files = [f for f in files if not f.name.startswith("US1")]

    print(f"📦 Remaining files to process: {len(files)}")
    results = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_one_file, f): f for f in files}
        for i, fut in enumerate(as_completed(futures), 1):
            df = fut.result()
            if df is not None:
                results.append(df)
            if i % 200 == 0:
                print(f"  ✅ Processed {i}/{len(files)} files...")

    if not results:
        print("⚠️ No matching stations found in region.")
        return

    combined = pd.concat(results, ignore_index=True)
    combined.sort_values(by=["STATION", "DATE"], inplace=True)
    combined.to_csv(OUT_CSV, index=False)

    print(f"\n✅ Saved {len(combined):,} records to {OUT_CSV}")
    print(f"📍 Total stations merged: {combined['STATION'].nunique()}")
    print(combined["PRCP"].describe())

if __name__ == "__main__":
    main()