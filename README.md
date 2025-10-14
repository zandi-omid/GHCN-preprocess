🌍 GHCN-Preprocess

A lightweight, parallel data-processing pipeline for GHCN (Global Historical Climatology Network)-Daily station datasets

⸻

📘 Overview

GHCN-Preprocess is a Python tool for efficiently reading, filtering, and merging NOAA’s Global Historical Climatology Network Daily (GHCN-Daily) station CSV files.
It provides a reproducible and customizable way to:
	•	Select a region of interest (ROI) by latitude/longitude bounds,
	•	Filter by date range,
	•	Apply measurement and quality flag screening,
	•	Exclude networks like CoCoRaHS, and
	•	Merge data from multiple countries (e.g., U.S. + Mexico).

The output is a clean, analysis-ready CSV containing precipitation data (PRCP) and station metadata.

⸻

⚙️ Features

✅ Parallel reading using ProcessPoolExecutor for large archives
✅ Quality-controlled filtering via MFLAG, QFLAG, and SFLAG
✅ Optional exclusion of CoCoRaHS (US1*) and other non-official stations
✅ Simple configuration — no external dependencies beyond pandas
✅ Easy extension for any region worldwide

🗂 Extracting Country-Specific Station Files

The archive contains one CSV per station (e.g., USC00012345.csv, MXM00076040.csv).
You can extract only the relevant stations for your country or region:

🇺🇸 Extract U.S. Stations

mkdir -p us_stations
tar -xzf daily-summaries-latest.tar.gz --wildcards -C us_stations "US*.csv"

