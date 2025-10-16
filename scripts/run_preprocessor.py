#!/usr/bin/env python3

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ghcn_preprocess import GHCNPreprocessor

if __name__ == "__main__":
    processor = GHCNPreprocessor(
        folder="/ra1/pubdat/ghcn_daily/study_area_countries_stations",
        out_csv="ghcn_precip_2005_2024_buffer.csv",
        min_lat=30,
        max_lat=38,
        min_lon=-115.5,
        max_lon=-108,
        start_date="2005-01-01",
        end_date="2024-12-31",
        exclude_prefixes=("US1"),  # exclude CoCoRaHS for now
        keep_mflags=["", "B", "N", "S", "T"],  # measurement flags to keep
        max_workers=24,
    )

    processor.run()