#!/usr/bin/env python3
# coding: utf-8
"""
GHCNPreprocessor
----------------
Combine, filter, and clean GHCN-Daily station CSVs for a given study region.

Features:
- Reads thousands of station CSVs in parallel
- Filters by latitude/longitude bounding box and date range
- Handles measurement flags (MFLAG) and quality flags (QFLAG)
- Optionally excludes community networks like CoCoRaHS
- Converts precipitation (PRCP) from tenths of mm → mm

Author: Omid Zandi
"""

import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed


class GHCNPreprocessor:
    def __init__(
        self,
        folder: str,
        out_csv: str,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        start_date: str = "2010-01-01",
        end_date: str = "2024-12-31",
        exclude_prefixes: tuple = ("US1",),
        keep_mflags: list = None,
        max_workers: int = 24,
    ):
        """
        Initialize the GHCN preprocessor.
        """
        self.folder = Path(folder)
        self.out_csv = Path(out_csv)
        self.min_lat, self.max_lat = min_lat, max_lat
        self.min_lon, self.max_lon = min_lon, max_lon
        self.start_date, self.end_date = start_date, end_date
        self.exclude_prefixes = exclude_prefixes
        self.max_workers = max_workers

        self.keep_mflags = (
            keep_mflags
            if keep_mflags is not None
            else ["", "G", "B", "N", "S", "T", "A", "E"]
        )

    # -------------------------------------------------------------------------
    @staticmethod
    def _split_flags(df: pd.DataFrame) -> pd.DataFrame:
        """Split PRCP_ATTRIBUTES into MFLAG, QFLAG, and SFLAG."""
        if "PRCP_ATTRIBUTES" not in df:
            df["MFLAG"] = df["QFLAG"] = df["SFLAG"] = ""
            return df

        flags = df["PRCP_ATTRIBUTES"].fillna("").astype(str).str.split(",", expand=True)
        df["MFLAG"] = flags[0].fillna("").str.strip()
        df["QFLAG"] = flags[1].fillna("").str.strip()
        df["SFLAG"] = flags[2].fillna("").str.strip()
        return df

    # -------------------------------------------------------------------------
    def _filter_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Measurement (MFLAG) and Quality (QFLAG) filtering."""
        df = self._split_flags(df)

        # Keep only valid MFLAGs and passed QFLAGs
        df = df[df["MFLAG"].isin(self.keep_mflags)]
        df = df[(df["QFLAG"].isna()) | (df["QFLAG"].str.strip() == "")]

        # Handle trace values
        df.loc[df["MFLAG"].isin(["S", "T"]), "PRCP"] = 0.0
        return df

    # -------------------------------------------------------------------------
    def _process_one_file(self, file: Path) -> pd.DataFrame | None:
        """Read, clean, and filter one station file."""
        try:
            # Exclude unwanted station types
            if file.name.startswith(self.exclude_prefixes):
                return None

            df = pd.read_csv(file, parse_dates=["DATE"])

            lat, lon = df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0]
            if not (self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon):
                return None

            df = df[(df["DATE"] >= self.start_date) & (df["DATE"] <= self.end_date)]
            if df.empty:
                return None

            df["PRCP"] = pd.to_numeric(df["PRCP"], errors="coerce") / 10.0
            df = self._filter_flags(df)
            if df.empty:
                return None

            return df

        except Exception as e:
            print(f"⚠️ Skipping {file.name}: {e}")
            return None

    # -------------------------------------------------------------------------
    def run(self):
        """Run the full preprocessing pipeline."""
        files = sorted(self.folder.glob("*.csv"))
        if not files:
            raise FileNotFoundError(f"No CSV files found in {self.folder}")

        print(f"🧩 Found {len(files)} station CSVs.")
        results = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self._process_one_file, f): f for f in files}
            for i, fut in enumerate(as_completed(futures), 1):
                df = fut.result()
                if df is not None:
                    results.append(df)
                if i % 200 == 0:
                    print(f"  ✅ Processed {i}/{len(files)} files...")

        if not results:
            print("⚠️ No stations matched filters.")
            return

        combined = pd.concat(results, ignore_index=True)
        combined.sort_values(by=["STATION", "DATE"], inplace=True)
        combined.to_csv(self.out_csv, index=False)

        print(f"\n✅ Saved {len(combined):,} records to {self.out_csv}")
        print(f"📍 Total stations merged: {combined['STATION'].nunique()}")
        print(combined["PRCP"].describe())