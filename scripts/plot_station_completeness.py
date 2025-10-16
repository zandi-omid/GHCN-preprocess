#!/usr/bin/env python3
# coding: utf-8
"""
Visualize GHCN-Daily station completeness (2005–2024)
----------------------------------------------------
- Loads merged daily precipitation CSV.
- Computes completeness (% of available days).
- Filters to stations with ≥80% data coverage.
- Plots histogram and map of station completeness.

Author: Omid Zandi
"""
#%%
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path



#%%
# ============================================================
# CONFIGURATION
# ============================================================
CSV_PATH = "../ghcn_precip_2005_2024_buffer.csv"
START_DATE = "2005-01-01"
END_DATE   = "2024-12-31"
MIN_LAT, MAX_LAT = 29.5, 39
MIN_LON, MAX_LON = -115.5, -107.5
COMPLETENESS_THRESHOLD = 80  # percent (stations with higher than this threshold are selected)
DPI = 300

#%%
# ============================================================
# LOAD DATA
# ============================================================
df = pd.read_csv(
    CSV_PATH,
    usecols=["STATION", "LATITUDE", "LONGITUDE", "PRCP"],
)

print(f"📥 Loaded {len(df):,} daily records across {df['STATION'].nunique()} stations.")

# --- Compute per-station observation counts ---
station_stats = (
    df.groupby("STATION")
      .agg(
          LATITUDE=("LATITUDE", "first"),
          LONGITUDE=("LONGITUDE", "first"),
          N_obs=("PRCP", "count"), 
          mean_prcp=("PRCP", "mean")   # mean daily precipitation (mm/day)
      )
      .reset_index()
)

# --- Compute total number of days in the selected range ---
n_days = (pd.to_datetime(END_DATE) - pd.to_datetime(START_DATE)).days + 1
station_stats["completeness"] = station_stats["N_obs"] / n_days * 100

# --- Filter stations with >= 80% completeness ---
filtered_stations = station_stats[station_stats["completeness"] >= COMPLETENESS_THRESHOLD]
print(f"\n📅 Total days in range: {n_days}")
print(f"📊 Total stations: {len(station_stats):,}")
print(f"✅ Stations with ≥{COMPLETENESS_THRESHOLD}% completeness: {len(filtered_stations):,}")

#%%
# ============================================================
# 1️⃣ HISTOGRAM OF COMPLETENESS
# ============================================================
plt.figure(figsize=(7, 4), dpi=DPI)
plt.hist(
    station_stats["completeness"],
    bins=40,
    color="royalblue",
    edgecolor="black",
    alpha=0.8,
)
plt.axvline(COMPLETENESS_THRESHOLD, color="crimson", linestyle="--", label=f"{COMPLETENESS_THRESHOLD}% threshold")
plt.xlabel("Data completeness (%)")
plt.ylabel("Number of stations")
plt.title("Distribution of GHCN-Daily Station Completeness (2005–2024)")
plt.legend()
plt.tight_layout()
plt.show()

#%%
# ============================================================
# 2️⃣ MAP OF STATIONS BY COMPLETENESS
# ============================================================
fig = plt.figure(figsize=(8, 7), dpi=DPI)
ax = plt.axes(projection=ccrs.PlateCarree())

ax.set_extent([MIN_LON, MAX_LON, MIN_LAT, MAX_LAT])

# ============================================================
# ✅ Verify and visualize bounding box on the map
# ============================================================
# # Print the actual extent used by Cartopy
# actual_extent = ax.get_extent(crs=ccrs.PlateCarree())
# print(f"🧭 Map extent (lon_min, lon_max, lat_min, lat_max): {actual_extent}")

# # Draw a red outline rectangle of the bounding box
# bbox_rect = mpatches.Rectangle(
#     (MIN_LON, MIN_LAT),                   # lower-left corner
#     width=(MAX_LON - MIN_LON),
#     height=(MAX_LAT - MIN_LAT),
#     transform=ccrs.PlateCarree(),
#     fill=False,
#     color="red",
#     linewidth=1.2,
#     linestyle="--",
#     label="Configured bounding box"
# )
# ax.add_patch(bbox_rect)

ax.add_feature(cfeature.LAND, facecolor="lightgray")
ax.add_feature(cfeature.OCEAN, facecolor="aliceblue")
ax.add_feature(cfeature.BORDERS, linewidth=0.5)
ax.add_feature(cfeature.STATES, linewidth=0.5)
gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
gl.top_labels = gl.right_labels = False

# --- Plot all stations faintly in gray ---
ax.scatter(
    station_stats["LONGITUDE"],
    station_stats["LATITUDE"],
    c="lightgray",
    s=20,
    alpha=0.5,
    transform=ccrs.PlateCarree(),
    label="All stations"
)

# --- Highlight high-completeness stations ---
sc = ax.scatter(
    filtered_stations["LONGITUDE"],
    filtered_stations["LATITUDE"],
    c=filtered_stations["completeness"],
    cmap="viridis",
    s=30 + 80 * (filtered_stations["completeness"] / 100),
    edgecolor="black",
    linewidth=0.3,
    alpha=0.9,
    transform=ccrs.PlateCarree(),
    label=f"≥{COMPLETENESS_THRESHOLD}% stations"
)

# --- Colorbar ---
cbar = plt.colorbar(sc, ax=ax, orientation="vertical", shrink=0.7, pad=0.03)
cbar.set_label("Data completeness (%)", fontsize=10)

plt.title(
    f"GHCN-Daily Station Completeness (2005–2024)\n"
    f"Stations with ≥{COMPLETENESS_THRESHOLD}% daily records",
    fontsize=13
)
plt.legend(loc="lower left", fontsize=8)
plt.tight_layout()
plt.show()
# %%

# Create shapefile directory if not exists
shp_dir = Path("/ra1/pubdat/ghcn_daily/outputs/shapefiles")
shp_dir.mkdir(parents=True, exist_ok=True)

# File paths
output_gpkg = shp_dir / "ghcn_stations_2005_2024_AZ_buffer_80pct.gpkg"
output_shp  = shp_dir / "ghcn_stations_2005_2024_AZ_buffer_80pct.shp"

# Create GeoDataFrame
geometry = [Point(xy) for xy in zip(filtered_stations["LONGITUDE"], filtered_stations["LATITUDE"])]
gdf = gpd.GeoDataFrame(
    filtered_stations,
    geometry=geometry,
    crs="EPSG:4326"
)

# Export both formats
gdf.to_file(output_gpkg, driver="GPKG")
gdf.to_file(output_shp, driver="ESRI Shapefile")

print(f"✅ GeoPackage saved: {output_gpkg}")
print(f"✅ Shapefile saved:  {output_shp}")
print(f"📍 Stations exported: {len(gdf)}")
print(gdf[["STATION", "completeness", "mean_prcp"]].head())
# %%
