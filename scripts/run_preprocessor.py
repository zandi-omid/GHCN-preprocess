#!/usr/bin/env python3
"""Example runner for GHCNPreprocessor."""

from ghcn_preprocess import GHCNPreprocessor

def main():
    processor = GHCNPreprocessor(
        folder="/ra1/pubdat/ghcn_daily/us_stations",
        out_csv="ghcn_precip_2010_2024_AZ_buffer_clean.csv",
        min_lat=30,
        max_lat=38,
        min_lon=-116,
        max_lon=-107,
        exclude_prefixes=("US1",),  # exclude CoCoRaHS
        max_workers=24,
    )
    processor.run()

if __name__ == "__main__":
    main()