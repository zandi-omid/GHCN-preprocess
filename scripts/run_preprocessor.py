#!/usr/bin/env python3
from ghcn_preprocess import GHCNPreprocessor

if __name__ == "__main__":
    processor = GHCNPreprocessor(
        folder="/ra1/pubdat/ghcn_daily/north_america_stations",  # merged folder
        out_csv="ghcn_precip_2010_2024_NA_buffer.csv",
        min_lat=20,   # widen a bit for US+MX
        max_lat=40,
        min_lon=-120,
        max_lon=-100,
        start_date="2010-01-01",
        end_date="2024-12-31",
        exclude_prefixes=("US1", "MXR"),  # exclude CoCoRaHS + other unwanted ones
        keep_mflags=["", "B", "N", "S", "T"],  # measurement flags to keep
        max_workers=24,
    )

    processor.run()