import io
from glob import glob
from datetime import datetime
import numpy as np
import pandas as pd
import geopandas as gpd

import matplotlib.pyplot as plt
import matplotlib.style as mpstyle
from matplotlib.ticker import ScalarFormatter
from matplotlib.dates import DateFormatter, MonthLocator
from matplotlib.patches import Patch, Rectangle
from matplotlib.lines import Line2D
from matplotlib.font_manager import FontProperties
import matplotlib.colors as mpcolors

from osgeo import ogr
import cartopy.crs as ccrs
from shapely.geometry import LineString, Point

from tathu.io import pgis
from tathu.geometry.transform import ogr2shapely
from tathu.geometry.utils import fitEllipse, extractCoordinates2NumpyArray

from my_secrets import postgis_pwd


def bytea2nparray(bytea):
    """Converts Numpy Array from Postgres to python."""
    bdata = io.BytesIO(bytea)
    bdata.seek(0)
    return np.load(bdata)


# Plot style
mpstyle.use("seaborn-whitegrid")
plt.rc("xtick", labelsize="small")
plt.rc("ytick", labelsize="small")

# OGR exceptions for conversions
ogr.UseExceptions()

# Rivers shapefile for map plots
rivers = gpd.read_file(
    "/home/camilacl/git/amazon-storms-aerosols/data/general/shapefiles/ne_10m_rivers_lake_centerlines.shp"
).to_crs("EPSG:3395")


# Load systems_filtered, systems
db = pgis.Loader(
    "localhost",
    "goamazon_geo",
    "camilacl",
    postgis_pwd("camilacl"),
    "systems_filtered",
)
db_full = pgis.Loader(
    "localhost", "goamazon_geo", "camilacl", postgis_pwd("camilacl"), "systems"
)
names = db.loadNames()
names_full = db_full.loadNames()
print("CS systems_filtered = #", len(names))
print("CS systems = #", len(names_full))

# Extracting data for statistics

# - From systems_filtered
print("From systems_filtered...")
# -- Cl Area
query = "SELECT count FROM systems_filtered ORDER BY name, date_time ASC"
areas = [q[0] for q in db.query(query)]
print("areas = #", len(areas))
# -- CS Max area
query = "SELECT MAX(count) FROM systems_filtered GROUP BY name"
maxplots = [q[0] for q in db.query(query)]
print(len(maxplots))
# -- CS name, duration
query = (
    "SELECT name, elapsed_time FROM (SELECT name, EXTRACT(epoch FROM"
    " (max(date_time) - min(date_time))/60) AS elapsed_time FROM"
    " systems_filtered GROUP BY name) AS duration"
)
names = [q[0] for q in db.query(query)]
durations = [q[1] for q in db.query(query)]
print("durations = #", len(durations))
# -- Cl max Z
query = "SELECT max FROM systems_filtered ORDER BY name, date_time ASC"
zmax = [q[0] for q in db.query(query)]
print("zmax = #", len(zmax))
# -- Cl mean Z
query = "SELECT mean FROM systems_filtered ORDER BY name, date_time ASC"
zmean = [q[0] for q in db.query(query)]
print("zmean = #", len(zmean))
# -- Cl classification
query = "SELECT event FROM systems_filtered ORDER BY name, date_time ASC"
event = [q[0] for q in db.query(query)]
print("event = #", len(event))
# -- Cl timestamp
query = "SELECT date_time FROM systems_filtered ORDER BY name, date_time ASC"
timestamp = [q[0] for q in db.query(query)]
print("timestamp = #", len(timestamp))
# -- CL GLD strokes
query = "SELECT gld FROM systems_filtered ORDER BY name, date_time ASC"
gld = [q[0] for q in db.query(query)]
print("gld = #", len(gld))
# -- CS first geom
query = (
    "SELECT DISTINCT ON (name) ST_AsBinary(geom) as wkb FROM systems_filtered"
    " ORDER BY name, date_time ASC"
)
first_geoms = [ogr.CreateGeometryFromWkb(bytes(q[0])) for q in db.query(query)]
print("first_geoms = #", len(first_geoms))
# -- CS first timestamp
query = (
    "SELECT DISTINCT ON (name) date_time FROM systems_filtered ORDER BY name,"
    " date_time ASC"
)
first_dates = [q[0] for q in db.query(query)]
print("first_dates = #", len(first_dates))
# -- CL names, geoms
query = (
    "SELECT name, ST_AsBinary(geom) as wkb FROM systems_filtered ORDER BY name,"
    " date_time ASC"
)
geoms_names = [q[0] for q in db.query(query)]
geoms = [ogr.CreateGeometryFromWkb(bytes(q[1])) for q in db.query(query)]
print("geoms = #", len(geoms))
# -- Cl Echo tops and variations
query = (
    "SELECT echotop_0, echotop_20, echotop_40, echotop0_kmmin, echotop20_kmmin,"
    " echotop40_kmmin FROM systems_filtered ORDER BY name, date_time ASC"
)
echo0 = [q[0] for q in db.query(query)]
print("echo0 = #", len(echo0))
echo20 = [q[1] for q in db.query(query)]
print("echo20 = #", len(echo20))
echo40 = [q[2] for q in db.query(query)]
print("echo40 = #", len(echo40))
decho0 = [q[3] for q in db.query(query)]
print("decho0 = #", len(decho0))
decho20 = [q[4] for q in db.query(query)]
print("decho20 = #", len(decho20))
decho40 = [q[5] for q in db.query(query)]
print("decho40 = #", len(decho40))
# -- Cl VIL, VII
query = (
    "SELECT vil_kgm2, vii_kgm2 FROM systems_filtered ORDER BY name,"
    " date_time ASC"
)
vil = [bytea2nparray(q[0]) / 10000 for q in db.query(query)]
print("vil = #", len(vil))
vii = [bytea2nparray(q[1]) / 10000 for q in db.query(query)]
print("vii = #", len(vii))
# -- Cl CFAD frequencies
query = "SELECT z_freq FROM systems_filtered ORDER BY name, date_time ASC"
zfreq = [bytea2nparray(q[0]) for q in db.query(query)]
print("zfreq = #", len(zfreq))
# -- Cl NAE
query = "SELECT nae_s_1 FROM systems_filtered ORDER BY name, date_time ASC"
nae = [q[0] for q in db.query(query)]
print("nae = #", len(nae))
# -- CL GLD variations
query = "SELECT gld_strmin FROM systems_filtered ORDER BY name, date_time ASC"
dgld = [q[0] for q in db.query(query)]
print("dgld = #", len(dgld))
print("dgld = #", len(dgld))
# -- Cl initiation
query = (
    "SELECT date_init25, totaerosol25_1_cm3, totccn25_1_cm3, cape25_j_kg,"
    " cin25_j_kg, blrh25_pc, lvws25_m_s FROM systems_filtered ORDER BY name,"
    " date_time ASC"
)
date_init25 = [q[0] for q in db.query(query)]
print("date_init25 = #", len(date_init25))
totaerosol25 = [q[1] for q in db.query(query)]
print("totaerosol25 = #", len(totaerosol25))
totccn25 = [q[2] for q in db.query(query)]
print("totccn25 = #", len(totccn25))
cape25 = [q[3] for q in db.query(query)]
print("cape25 = #", len(cape25))
cin25 = [q[4] for q in db.query(query)]
print("cin25 = #", len(cin25))
blrh25 = [q[5] for q in db.query(query)]
print("blrh25 = #", len(blrh25))
lvws25 = [q[6] for q in db.query(query)]
print("lvws25 = #", len(lvws25))
query = (
    "SELECT DISTINCT ON (name) date_init25, totaerosol25_1_cm3, totccn25_1_cm3,"
    " cape25_j_kg, cin25_j_kg, blrh25_pc, lvws25_m_s FROM systems_filtered"
    " ORDER BY name, date_time ASC"
)
date_init25_per = [q[0] for q in db.query(query)]
print("date_init25_per = #", len(date_init25_per))
totaerosol25_per = [q[1] for q in db.query(query)]
print("totaerosol25_per = #", len(totaerosol25_per))
totccn25_per = [q[2] for q in db.query(query)]
print("totccn25_per = #", len(totccn25_per))
cape25_per = [q[3] for q in db.query(query)]
print("cape25_per = #", len(cape25_per))
cin25_per = [q[4] for q in db.query(query)]
print("cin25_per = #", len(cin25_per))
blrh25_per = [q[5] for q in db.query(query)]
print("blrh25_per = #", len(blrh25_per))
lvws25_per = [q[6] for q in db.query(query)]
print("lvws25_per = #", len(lvws25_per))
query = (
    "SELECT date_init10, totaerosol10_1_cm3, totccn10_1_cm3, cape10_j_kg,"
    " cin10_j_kg, blrh10_pc, lvws10_m_s FROM systems_filtered ORDER BY name,"
    " date_time ASC"
)
date_init10 = [q[0] for q in db.query(query)]
print("date_init10 = #", len(date_init10))
totaerosol10 = [q[1] for q in db.query(query)]
print("totaerosol10 = #", len(totaerosol10))
totccn10 = [q[2] for q in db.query(query)]
print("totccn10 = #", len(totccn10))
cape10 = [q[3] for q in db.query(query)]
print("cape10 = #", len(cape10))
cin10 = [q[4] for q in db.query(query)]
print("cin10 = #", len(cin10))
blrh10 = [q[5] for q in db.query(query)]
print("blrh10 = #", len(blrh10))
lvws10 = [q[6] for q in db.query(query)]
print("lvws10 = #", len(lvws10))
query = (
    "SELECT DISTINCT ON (name) date_init10, totaerosol10_1_cm3, totccn10_1_cm3,"
    " cape10_j_kg, cin10_j_kg, blrh10_pc, lvws10_m_s FROM systems_filtered"
    " ORDER BY name, date_time ASC"
)
date_init10_per = [q[0] for q in db.query(query)]
print("date_init10_per = #", len(date_init10_per))
totaerosol10_per = [q[1] for q in db.query(query)]
print("totaerosol10_per = #", len(totaerosol10_per))
totccn10_per = [q[2] for q in db.query(query)]
print("totccn10_per = #", len(totccn10_per))
cape10_per = [q[3] for q in db.query(query)]
print("cape10_per = #", len(cape10_per))
cin10_per = [q[4] for q in db.query(query)]
print("cin10_per = #", len(cin10_per))
blrh10_per = [q[5] for q in db.query(query)]
print("blrh10_per = #", len(blrh10_per))
lvws10_per = [q[6] for q in db.query(query)]
print("lvws10_per = #", len(lvws10_per))

# - From systems
print("From systems...")
# -- Cl areas
query = (
    "SELECT count FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a')"
    "  ORDER BY name, date_time ASC"
)
areas_full = [q[0] for q in db_full.query(query)]
print("areas_full = #", len(areas_full))
# -- CS max area
query = (
    "SELECT MAX(count) FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a') GROUP BY name"
    "  ORDER BY name"
)
maxplots_full = [q[0] for q in db_full.query(query)]
print("maxplots_full = #", len(maxplots_full))
# -- CS duration
query = (
    "SELECT elapsed_time, name FROM (SELECT name, EXTRACT(epoch FROM"
    " (max(date_time) - min(date_time))/60) AS elapsed_time FROM systems WHERE"
    " name NOT IN ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a') GROUP BY name ORDER BY name) AS"
    " duration"
)
durations_full = [q[0] for q in db_full.query(query)]
namesd_full = [q[1] for q in db_full.query(query)]
print("durations_full = #", len(durations_full))
# -- Cl max Z
query = (
    "SELECT max FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a')"
    "  ORDER BY name, date_time ASC"
)
zmax_full = [q[0] for q in db_full.query(query)]
print("zmax_full = #", len(zmax_full))
# -- Cl mean Z
query = (
    "SELECT mean FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a')"
    "  ORDER BY name, date_time ASC"
)
zmean_full = [q[0] for q in db_full.query(query)]
print("zmean_full = #", len(zmean_full))
# -- Cl classification
query = (
    "SELECT event FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a')"
    "  ORDER BY name, date_time ASC"
)
event_full = [q[0] for q in db_full.query(query)]
print("event_full = #", len(event_full))
# -- CL timestamp
query = (
    "SELECT date_time, name FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a')"
    "  ORDER BY name, date_time ASC"
)
timestamp_full = [q[0] for q in db_full.query(query)]
namest_full = [q[1] for q in db_full.query(query)]
print("timestamp_full = #", len(timestamp_full))
# -- CS first timestamp
query = (
    "SELECT DISTINCT ON (name) date_time FROM systems WHERE name NOT IN"
    " ('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    " '1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    " 'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    " '50f14b96-efa7-4dee-aa1b-510ece86172a') ORDER BY name, date_time ASC"
)
first_dates_full = [q[0] for q in db_full.query(query)]
print("first_dates_full = #", len(first_dates_full))

# Converting to pandas dfs

# - From systems_filtered
systems_all = pd.DataFrame(
    {
        "area": areas,
        "max": zmax,
        "mean": zmean,
        "event": event,
        "timestamp": timestamp,
        "gld": gld,
        "geom_name": geoms_names,
        "geom": geoms,
        "echotop_0": echo0,
        "echotop_20": echo20,
        "echotop_40": echo40,
        "dechotop_0": decho0,
        "dechotop_20": decho20,
        "dechotop_40": decho40,
        "vil": vil,
        "vii": vii,
        "zfreq": zfreq,
        "nae": nae,
        "dgld": dgld,
    }
)
systems_all.timestamp = systems_all.timestamp.dt.tz_localize("UTC")
systems_all.timestamp = systems_all.timestamp.dt.tz_convert("America/Manaus")
systems_all.geom = [ogr2shapely(g) for g in systems_all.geom]
systems_all.nae = systems_all.nae * 1e6
systems_all[["echotop_0", "echotop_20", "echotop_40"]] = systems_all[
    ["echotop_0", "echotop_20", "echotop_40"]
].astype(int)
systems_all[["dechotop_0", "dechotop_20", "dechotop_40", "dgld"]] = systems_all[
    ["dechotop_0", "dechotop_20", "dechotop_40", "dgld"]
].round(1)
systems_per = pd.DataFrame(
    {
        "name": names,
        "maxplot": maxplots,
        "duration": [float(dur) / 60 for dur in durations],
        "geom": first_geoms,
        "timestamp": first_dates,
    }
)
systems_per.timestamp = systems_per.timestamp.dt.tz_localize("UTC")
systems_per.timestamp = systems_per.timestamp.dt.tz_convert("America/Manaus")
systems_per.geom = [ogr2shapely(g) for g in systems_per.geom]
systems_all_init25 = pd.concat(
    [
        systems_all,
        pd.DataFrame(
            {
                "date_init25": date_init25,
                "totaerosol25": totaerosol25,
                "totccn25": totccn25,
                "cape25": cape25,
                "cin25": cin25,
                "blrh25": blrh25,
                "lvws25": lvws25,
            }
        ),
    ],
    axis=1,
).dropna(subset=["totaerosol25"])
systems_per_init10 = pd.concat(
    [
        systems_per,
        pd.DataFrame(
            {
                "date_init10": date_init10_per,
                "totaerosol10": totaerosol10_per,
                "totccn10": totccn10_per,
                "cape10": cape10_per,
                "cin10": cin10_per,
                "blrh10": blrh10_per,
                "lvws10": lvws10_per,
            }
        ),
    ],
    axis=1,
).dropna(subset=["totaerosol10"])
systems_per_init25 = pd.concat(
    [
        systems_per,
        pd.DataFrame(
            {
                "date_init25": date_init25_per,
                "totaerosol25": totaerosol25_per,
                "totccn25": totccn25_per,
                "cape25": cape25_per,
                "cin25": cin25_per,
                "blrh25": blrh25_per,
                "lvws25": lvws25_per,
            }
        ),
    ],
    axis=1,
).dropna(subset=["totaerosol25"])
systems_all_init10 = pd.concat(
    [
        systems_all,
        pd.DataFrame(
            {
                "date_init10": date_init10,
                "totaerosol10": totaerosol10,
                "totccn10": totccn10,
                "cape10": cape10,
                "cin10": cin10,
                "blrh10": blrh10,
                "lvws10": lvws10,
            }
        ),
    ],
    axis=1,
).dropna(subset=["totaerosol10"])

# - From systems
systems_all_full = pd.DataFrame(
    {
        "name": namest_full,
        "area": areas_full,
        "max": zmax_full,
        "mean": zmean_full,
        "event": event_full,
        "timestamp": timestamp_full,
    }
)
systems_all_full.timestamp = systems_all_full.timestamp.dt.tz_localize("UTC")
systems_all_full.timestamp = systems_all_full.timestamp.dt.tz_convert(
    "America/Manaus"
)
systems_per_full = pd.DataFrame(
    {
        "name": namesd_full,
        "maxplot": maxplots_full,
        "duration": [float(dur) / 60 for dur in durations_full],
        "timestamp": first_dates_full,
    }
)
systems_per_full.timestamp = systems_per_full.timestamp.dt.tz_localize("UTC")
systems_per_full.timestamp = systems_per_full.timestamp.dt.tz_convert(
    "America/Manaus"
)

# - Filtered versions

# -- Clusters per season, IOPs
# --- Definition: Machado et al. (2018)
systems_all_full_wet = systems_all_full.loc[
    systems_all_full["timestamp"].dt.month.isin([1, 2, 3])
]
systems_all_wet = systems_all.loc[
    systems_all["timestamp"].dt.month.isin([1, 2, 3])
]
systems_all_full_dry = systems_all_full.loc[
    systems_all_full["timestamp"].dt.month.isin([8, 9, 10])
]
systems_all_dry = systems_all.loc[
    systems_all["timestamp"].dt.month.isin([8, 9, 10])
]
systems_all_full_drytowet = systems_all_full.loc[
    systems_all_full["timestamp"].dt.month.isin([11, 12])
]
systems_all_drytowet = systems_all.loc[
    systems_all["timestamp"].dt.month.isin([11, 12])
]
systems_all_full_iop1 = systems_all_full.loc[
    (systems_all_full["timestamp"].dt.month.isin([2, 3]))
    & (systems_all_full["timestamp"].dt.year == 2014)
]
systems_all_iop1 = systems_all.loc[
    (systems_all["timestamp"].dt.month.isin([2, 3]))
    & (systems_all["timestamp"].dt.year == 2014)
]
systems_all_full_iop2 = (
    systems_all_full.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)
systems_all_iop2 = (
    systems_all.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)
systems_all_init25_wet = systems_all_init25.loc[
    systems_all_init25["timestamp"].dt.month.isin([1, 2, 3])
]
systems_all_init10_wet = systems_all_init10.loc[
    systems_all_init10["timestamp"].dt.month.isin([1, 2, 3])
]
systems_all_init25_dry = systems_all_init25.loc[
    systems_all_init25["timestamp"].dt.month.isin([8, 9, 10])
]
systems_all_init10_dry = systems_all_init10.loc[
    systems_all_init10["timestamp"].dt.month.isin([8, 9, 10])
]
systems_all_init25_drytowet = systems_all_init25.loc[
    systems_all_init25["timestamp"].dt.month.isin([11, 12])
]
systems_all_init10_drytowet = systems_all_init10.loc[
    systems_all["timestamp"].dt.month.isin([11, 12])
]
systems_all_init25_iop1 = systems_all_init25.loc[
    (systems_all_init25["timestamp"].dt.month.isin([2, 3]))
    & (systems_all_init25["timestamp"].dt.year == 2014)
]
systems_all_init10_iop1 = systems_all_init10.loc[
    (systems_all_init10["timestamp"].dt.month.isin([2, 3]))
    & (systems_all_init10["timestamp"].dt.year == 2014)
]
systems_all_init25_iop2 = (
    systems_all_init25.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)
systems_all_init10_iop2 = (
    systems_all_init10.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)

# -- CS per season, IOPs
# --- Definition: Machado et al. (2018)
systems_per_full_wet = systems_per_full.loc[
    systems_per_full["timestamp"].dt.month.isin([1, 2, 3])
]
systems_per_wet = systems_per.loc[
    systems_per["timestamp"].dt.month.isin([1, 2, 3])
]
systems_per_init25_wet = systems_per_init25.loc[
    systems_per_init25["timestamp"].dt.month.isin([1, 2, 3])
]
systems_per_init10_wet = systems_per_init10.loc[
    systems_per_init10["timestamp"].dt.month.isin([1, 2, 3])
]
systems_per_full_dry = systems_per_full.loc[
    systems_per_full["timestamp"].dt.month.isin([8, 9, 10])
]
systems_per_dry = systems_per.loc[
    systems_per["timestamp"].dt.month.isin([8, 9, 10])
]
systems_per_init25_dry = systems_per_init25.loc[
    systems_per_init25["timestamp"].dt.month.isin([8, 9, 10])
]
systems_per_init10_dry = systems_per_init10.loc[
    systems_per_init10["timestamp"].dt.month.isin([8, 9, 10])
]
systems_per_full_drytowet = systems_per_full.loc[
    systems_per_full["timestamp"].dt.month.isin([11, 12])
]
systems_per_drytowet = systems_per.loc[
    systems_per["timestamp"].dt.month.isin([11, 12])
]
systems_per_init25_drytowet = systems_per_init25.loc[
    systems_per_init25["timestamp"].dt.month.isin([11, 12])
]
systems_per_init10_drytowet = systems_per_init10.loc[
    systems_per_init10["timestamp"].dt.month.isin([11, 12])
]
systems_per_full_iop1 = systems_per_full.loc[
    (systems_per_full["timestamp"].dt.month.isin([2, 3]))
    & (systems_per_full["timestamp"].dt.year == 2014)
]
systems_per_iop1 = systems_per.loc[
    (systems_per["timestamp"].dt.month.isin([2, 3]))
    & (systems_per["timestamp"].dt.year == 2014)
]
systems_per_init25_iop1 = systems_per_init25.loc[
    (systems_per_init25["timestamp"].dt.month.isin([2, 3]))
    & (systems_per_init25["timestamp"].dt.year == 2014)
]
systems_per_init10_iop1 = systems_per_init10.loc[
    (systems_per_init10["timestamp"].dt.month.isin([2, 3]))
    & (systems_per_init10["timestamp"].dt.year == 2014)
]
systems_per_full_iop2 = (
    systems_per_full.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)
systems_per_iop2 = (
    systems_per.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)
systems_per_init25_iop2 = (
    systems_per_init25.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)
systems_per_init10_iop2 = (
    systems_per_init10.set_index(["timestamp"])
    .loc["2014-8-15":"2014-10-15"]
    .reset_index()
)

# -- CS per durations
names_2h = systems_per.loc[systems_per.duration <= 2].name
names_4h = systems_per.loc[
    (systems_per.duration > 2) & (systems_per.duration <= 4)
].name
names_6h = systems_per.loc[
    (systems_per.duration > 4) & (systems_per.duration <= 6)
].name
names_maxh = systems_per.loc[systems_per.duration > 6].name

# -- Names of CS without lighning
nogld_df = systems_all.groupby("geom_name").sum()
nogld_names = nogld_df.loc[nogld_df.gld == 0].index.to_list()

# PLOTS
# - Path to save figures
figpath = "./sipam-tracking/out/goamazon/figs/"

# Plot labels and custom legends

# -- Labels
class_labels = ["SPONTANEOUS\nGENERATION", "CONTINUITY", "SPLIT", "MERGE"]
dur_labels = ["≤ 1", "≤ 2", "≤ 3", "≤ 4", "≤ 5", "≤ 6", "> 6"]
time_ticks = pd.date_range(start="2014-01-01", end="2015-12-31", freq="1M")
time_labels = time_ticks.strftime("%b %Y")
time_ticks_init = pd.date_range(start="2014-01-01", end="2015-11-30", freq="1M")
time_labels_init = time_ticks_init.strftime("%b %Y")
time_ticks_hour = pd.date_range(start="00:00", end="23:00", freq="1H")
time_labels_hour = time_ticks_hour.strftime("%H:%M")
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
months_names = [
    "Jan",
    "Feb",
    "Mar",
    "Abr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]
figlabels = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
geolabels = [
    "E",
    "ENE",
    "NE",
    "NNE",
    "N",
    "NNW",
    "NW",
    "WNW",
    "W",
    "WSW",
    "SW",
    "SSW",
    "S",
    "SSE",
    "SE",
    "ESE",
]

# -- Legends
legend_rf = [
    Patch(facecolor="w", edgecolor="k", label="Raw"),
    Patch(facecolor="k", edgecolor="k", label="Filtered"),
]
legend_rf_init = [
    Patch(facecolor="g", edgecolor="k", label="Initiation within 25km"),
    Patch(facecolor="b", edgecolor="k", label="Initiation within 10km"),
]
legend_rfall = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all)) + ")",
    ),
]
legend_rfper = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per)) + ")",
    ),
]
legend_rfall_wet = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full_wet)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all_wet)) + ")",
    ),
]
legend_rfall_dry = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full_dry)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all_dry)) + ")",
    ),
]
legend_rfall_drytowet = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full_drytowet)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all_drytowet)) + ")",
    ),
]
legend_rfall_iop1 = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full_iop1)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all_iop1)) + ")",
    ),
]
legend_rfall_iop2 = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full_iop2)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all_iop2)) + ")",
    ),
]
legend_rfper_wet = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full_wet)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per_wet)) + ")",
    ),
]
legend_rfper_dry = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full_dry)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per_dry)) + ")",
    ),
]
legend_rfper_drytowet = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full_drytowet)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per_drytowet)) + ")",
    ),
]
legend_rfper_iop1 = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full_iop1)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per_iop1)) + ")",
    ),
]
legend_rfper_iop2 = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full_iop2)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per_iop2)) + ")",
    ),
]
legend_rfall_init = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_all_init25))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_all_init10))
        + ")",
    ),
]
legend_rfper_init = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_per_init25))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_per_init10))
        + ")",
    ),
]
legend_rfall_init_wet = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_all_init25_wet))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_all_init10_wet))
        + ")",
    ),
]
legend_rfall_init_dry = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_all_init25_dry))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_all_init10_dry))
        + ")",
    ),
]
legend_rfall_init_drytowet = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_all_init25_drytowet))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_all_init10_drytowet))
        + ")",
    ),
]
legend_rfall_init_iop1 = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_all_init25_iop1))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_all_init10_iop1))
        + ")",
    ),
]
legend_rfall_init_iop2 = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_all_init25_iop2))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_all_init10_iop2))
        + ")",
    ),
]
legend_rfper_init_wet = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_per_init25_wet))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_per_init10_wet))
        + ")",
    ),
]
legend_rfper_init_dry = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_per_init25_dry))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_per_init10_dry))
        + ")",
    ),
]
legend_rfper_init_drytowet = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_per_init25_drytowet))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_per_init10_drytowet))
        + ")",
    ),
]
legend_rfper_init_iop1 = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_per_init25_iop1))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_per_init10_iop1))
        + ")",
    ),
]
legend_rfper_init_iop2 = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="Initiation within 25km (total = "
        + str(len(systems_per_init25_iop2))
        + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="Initiation within 10km (total = "
        + str(len(systems_per_init10_iop2))
        + ")",
    ),
]

legend_rfall_mon = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_all_full)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_all)) + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_rfper_mon = [
    Patch(
        facecolor="w",
        edgecolor="k",
        label="Raw (total = " + str(len(systems_per_full)) + ")",
    ),
    Patch(
        facecolor="k",
        edgecolor="k",
        label="Filtered (total = " + str(len(systems_per)) + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_rfall_init_mon = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="25km (total = " + str(len(systems_all_init25)) + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="10km (total = " + str(len(systems_all_init10)) + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_rfper_init_mon = [
    Patch(
        facecolor="g",
        edgecolor="k",
        label="25km (total = " + str(len(systems_per_init25)) + ")",
    ),
    Patch(
        facecolor="b",
        edgecolor="k",
        label="10km (total = " + str(len(systems_per_init10)) + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_seasons = [
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_ngld_seasons = [
    Patch(
        facecolor="gold",
        edgecolor="k",
        label="Lightning ("
        + str(len(systems_all.loc[systems_all.gld > 0]))
        + ")",
    ),
    Patch(
        facecolor="gray",
        edgecolor="k",
        label="No lightning ("
        + str(len(systems_all.loc[systems_all.gld == 0]))
        + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_ngldcs_seasons = [
    Patch(
        facecolor="gold",
        edgecolor="k",
        label="Lightning ("
        + str(
            len(
                systems_all.loc[systems_all.gld > 0]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="gray",
        edgecolor="k",
        label="No lightning (" + str(len(nogld_names)) + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="dodgerblue",
        alpha=0.5,
        label="Wet season",
    ),
    Patch(facecolor="red", edgecolor="red", alpha=0.5, label="Dry season"),
]
legend_gldcl_seasons = [
    Patch(
        facecolor="red",
        edgecolor="k",
        label="Dry (total = "
        + str(len(systems_all_dry.loc[systems_all_dry.gld > 0]))
        + ")",
    ),
    Patch(
        facecolor="yellowgreen",
        edgecolor="k",
        label="Dry-to-Wet (total = "
        + str(len(systems_all_drytowet.loc[systems_all_drytowet.gld > 0]))
        + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="Wet (total = "
        + str(len(systems_all_wet.loc[systems_all_wet.gld > 0]))
        + ")",
    ),
]
legend_gldinit_seasons = [
    Patch(
        facecolor="red",
        edgecolor="k",
        label="Dry (total = "
        + str(
            len(
                systems_all_dry.loc[systems_all_dry.gld > 0]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="yellowgreen",
        edgecolor="k",
        label="Dry-to-Wet (total = "
        + str(
            len(
                systems_all_drytowet.loc[systems_all_drytowet.gld > 0]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="Wet (total = "
        + str(
            len(
                systems_all_wet.loc[systems_all_wet.gld > 0]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
]
legend_gld_seasons = [
    Patch(
        facecolor="red",
        edgecolor="k",
        label="Dry (total = " + str(int(systems_all_dry.gld.sum())) + ")",
    ),
    Patch(
        facecolor="yellowgreen",
        edgecolor="k",
        label="Dry-to-Wet (total = "
        + str(int(systems_all_drytowet.gld.sum()))
        + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="Wet (total = " + str(int(systems_all_wet.gld.sum())) + ")",
    ),
]
legend_nogldcl_seasons = [
    Patch(
        facecolor="red",
        edgecolor="k",
        label="Dry (total = "
        + str(len(systems_all_dry.loc[systems_all_dry.gld == 0]))
        + ")",
    ),
    Patch(
        facecolor="yellowgreen",
        edgecolor="k",
        label="Dry-to-Wet (total = "
        + str(len(systems_all_drytowet.loc[systems_all_drytowet.gld == 0]))
        + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="Wet (total = "
        + str(len(systems_all_wet.loc[systems_all_wet.gld == 0]))
        + ")",
    ),
]
legend_nogldinit_seasons = [
    Patch(
        facecolor="red",
        edgecolor="k",
        label="Dry (total = "
        + str(
            len(
                systems_all_dry.loc[systems_all_dry.geom_name.isin(nogld_names)]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="yellowgreen",
        edgecolor="k",
        label="Dry-to-Wet (total = "
        + str(
            len(
                systems_all_drytowet.loc[
                    systems_all_drytowet.geom_name.isin(nogld_names)
                ]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="Wet (total = "
        + str(
            len(
                systems_all_wet.loc[systems_all_wet.geom_name.isin(nogld_names)]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
]
legend_gldcl_iops = [
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="IOP1 (Wet Season) (total = "
        + str(len(systems_all_iop1.loc[systems_all_iop1.gld > 0]))
        + ")",
    ),
    Patch(
        facecolor="red",
        edgecolor="k",
        label="IOP2 (Dry Season) (total = "
        + str(len(systems_all_iop2.loc[systems_all_iop2.gld > 0]))
        + ")",
    ),
]
legend_gldinit_iops = [
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="IOP1 (Wet Season) (total = "
        + str(
            len(
                systems_all_iop1.loc[systems_all_iop1.gld > 0]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="red",
        edgecolor="k",
        label="IOP2 (Dry Season) (total = "
        + str(
            len(
                systems_all_iop2.loc[systems_all_iop2.gld > 0]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
]
legend_gld_iops = [
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="IOP1 (Wet Season) (total = "
        + str(int(systems_all_iop1.gld.sum()))
        + ")",
    ),
    Patch(
        facecolor="red",
        edgecolor="k",
        label="IOP2 (Dry Season) (total = "
        + str(int(systems_all_iop2.gld.sum()))
        + ")",
    ),
]
legend_nogldcl_iops = [
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="IOP1 (Wet Season) (total = "
        + str(len(systems_all_iop1.loc[systems_all_iop1.gld == 0]))
        + ")",
    ),
    Patch(
        facecolor="red",
        edgecolor="k",
        label="IOP2 (Dry Season) (total = "
        + str(len(systems_all_iop2.loc[systems_all_iop2.gld == 0]))
        + ")",
    ),
]
legend_nogldinit_iops = [
    Patch(
        facecolor="dodgerblue",
        edgecolor="k",
        label="IOP1 (Wet Season) (total = "
        + str(
            len(
                systems_all_iop1.loc[
                    systems_all_iop1.geom_name.isin(nogld_names)
                ]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
    Patch(
        facecolor="red",
        edgecolor="k",
        label="IOP2 (Dry Season) (total = "
        + str(
            len(
                systems_all_iop2.loc[
                    systems_all_iop2.geom_name.isin(nogld_names)
                ]
                .groupby("geom_name")
                .first()
            )
        )
        + ")",
    ),
]
legend_pmap = [
    Line2D(
        [0],
        [0],
        marker="o",
        color="w",
        label="Initiation",
        markerfacecolor="r",
        markersize=5,
    )
]

print(systems_per_init25.describe())
# print(systems_per_init25['duration'].quantile(q=0.1))

"""
# 1. Area
print("---- Plotting area ----")
fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = systems_all_full.area.plot.hist(
    bins=range(0, 63000, 1000), grid=True, color="w", edgecolor="k", ax=ax1
)
systems_all.area.plot.hist(
    bins=range(0, 63000, 1000), grid=True, color="k", edgecolor="k", ax=ax1
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_title("Area of Clusters")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rf)

ax2 = fig.add_subplot(gs[1, 0])
axplot = systems_per_full.maxplot.plot.hist(
    bins=range(0, 63000, 1000), grid=True, color="w", edgecolor="k", ax=ax2
)
systems_per.maxplot.plot.hist(
    bins=range(0, 63000, 1000), grid=True, color="k", edgecolor="k", ax=ax2
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_xlabel("km²")
axplot.set_ylabel("Count")
axplot.set_title("Max Area of Convective Systems")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rf)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_area.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

print("---- Plotting area init ----")
fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = systems_all_init25.area.plot.hist(
    bins=range(0, 5000, 250), grid=True, color="g", edgecolor="k", ax=ax1
)
systems_all_init10.area.plot.hist(
    bins=range(0, 5000, 250), grid=True, color="b", edgecolor="k", ax=ax1
)
# axplot.set_yscale("log")
# axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_title("Area of Clusters")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rf_init)

ax2 = fig.add_subplot(gs[1, 0])
axplot = systems_per_init25.maxplot.plot.hist(
    bins=range(0, 5000, 250), grid=True, color="g", edgecolor="k", ax=ax2
)
systems_per_init10.maxplot.plot.hist(
    bins=range(0, 5000, 250), grid=True, color="b", edgecolor="k", ax=ax2
)
# axplot.set_yscale("log")
# axplot.set_ylim(bottom=1)
axplot.set_xlabel("km²")
axplot.set_ylabel("Count")
axplot.set_title("Max Area of Convective Systems")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rf_init)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_area_init.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

# 2. Mean/max reflectivity
print("---- Plotting mean/max Z ----")
fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = systems_all_full[["max", "mean"]].plot.hist(
    bins=range(20, 70, 5),
    grid=True,
    weights=np.ones(len(systems_all_full)) / len(systems_all_full) * 100,
    color=("#3C2692", "#F1F1F1"),
    edgecolor="k",
    alpha=0.5,
    ax=ax1,
)
axplot.set_ylim((0, 45))
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Raw clusters (total = " + str(len(systems_all_full)) + ")")
axplot.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1, 0])
axplot = systems_all[["max", "mean"]].plot.hist(
    bins=range(20, 70, 5),
    grid=True,
    weights=np.ones(len(systems_all)) / len(systems_all) * 100,
    color=("#3C2692", "#F1F1F1"),
    edgecolor="k",
    alpha=0.5,
    ax=ax2,
)
axplot.set_ylim((0, 45))
axplot.set_ylabel("Frequency (%)")
axplot.set_xlabel("dBZ")
axplot.set_title("Filtered clusters (total = " + str(len(systems_all)) + ")")
axplot.set_title("b", loc="left", fontweight="bold", size=16)

fig.suptitle("3-km Reflectivity", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(figpath + "exploratory_stats_z.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

print("---- Plotting mean/max Z init ----")
fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = systems_all_init25[["max", "mean"]].plot.hist(
    bins=range(20, 70, 5),
    grid=True,
    weights=np.ones(len(systems_all_init25)) / len(systems_all_init25) * 100,
    color=("#3C2692", "#F1F1F1"),
    edgecolor="k",
    alpha=0.5,
    ax=ax1,
)
axplot.set_ylim((0, 45))
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Initiation within 25km (total = " + str(len(systems_all_init25)) + ")")
axplot.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1, 0])
axplot = systems_all_init10[["max", "mean"]].plot.hist(
    bins=range(20, 70, 5),
    grid=True,
    weights=np.ones(len(systems_all_init10)) / len(systems_all_init10) * 100,
    color=("#3C2692", "#F1F1F1"),
    edgecolor="k",
    alpha=0.5,
    ax=ax2,
)
axplot.set_ylim((0, 45))
axplot.set_ylabel("Frequency (%)")
axplot.set_xlabel("dBZ")
axplot.set_title(
    "Initiation within 10km (total = "
    + str(len(systems_all_init10))
    + ")"
)
axplot.set_title("b", loc="left", fontweight="bold", size=16)

fig.suptitle("3-km Reflectivity", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(figpath + "exploratory_stats_z_init.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

# 3. Classification
print("---- Plotting classification ----")

# - Extracting first timestamps for true spontaneous generation
files = glob("/home/camilacl/git/tathu/sipam-tracking/in/goamazon/*")
dt = []
for file in files:
    filelist = open(file, "r")
    dt.append(
        datetime.strptime(
            [line.strip() for line in filelist][0][61:73], "%Y%m%d%H%M"
        )
    )
dts = pd.Series(dt)
dts = dts.dt.tz_localize("UTC")
dts = dts.dt.tz_convert("America/Manaus")

# - Generating table
classes = pd.DataFrame(
    {
        "Raw": len(systems_all_full.groupby("name")),
        "Filtered": len(systems_all.groupby("geom_name")),
    },
    index=["Convective Systems (total)"],
)
classes.loc["CS spontaneously generated (%)"] = [
    len(
        systems_all_full.loc[
            (systems_all_full.event == "SPONTANEOUS_GENERATION")
            & (~systems_all_full.timestamp.isin(dts))
        ]
    )
    / classes["Raw"][0]
    * 100,
    len(
        systems_all.loc[
            (systems_all.event == "SPONTANEOUS_GENERATION")
            & (~systems_all.timestamp.isin(dts))
        ]
    )
    / classes["Filtered"][0]
    * 100,
]
classes.loc["CS with split/merge (%)"] = [
    len(
        systems_all_full.loc[
            systems_all_full.event.isin(["SPLIT", "MERGE"])
        ].groupby("name")
    )
    / classes["Raw"][0]
    * 100,
    len(
        systems_all.loc[systems_all.event.isin(["SPLIT", "MERGE"])].groupby(
            "geom_name"
        )
    )
    / classes["Filtered"][0]
    * 100,
]
classes.loc["CS with full lifecycle (%)"] = [
    len(
        systems_all_full.sort_values("timestamp")
        .groupby("name")
        .tail(1)
        .set_index("event")
        .loc["CONTINUITY"]
    )
    / classes["Raw"][0]
    * 100,
    len(
        systems_all.sort_values("timestamp")
        .groupby("geom_name")
        .tail(1)
        .set_index("event")
        .loc["CONTINUITY"]
    )
    / classes["Filtered"][0]
    * 100,
]

fig, ax = plt.subplots(figsize=(6, 2))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

table = ax.table(
    cellText=classes.values.astype(int),
    colLabels=classes.columns,
    rowLabels=classes.index,
    loc="center",
    colWidths=[0.2, 0.2],
)
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))

fig.tight_layout()

plt.savefig(figpath + "exploratory_stats_class.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, ax = [None] * 2

print("---- Plotting classification init ----")

# - Extracting first timestamps for true spontaneous generation
files = glob("/home/camilacl/git/tathu/sipam-tracking/in/goamazon/*")
dt = []
for file in files:
    filelist = open(file, "r")
    dt.append(
        datetime.strptime(
            [line.strip() for line in filelist][0][61:73], "%Y%m%d%H%M"
        )
    )
dts = pd.Series(dt)
dts = dts.dt.tz_localize("UTC")
dts = dts.dt.tz_convert("America/Manaus")

# - Generating table
classes = pd.DataFrame(
    {
        "Initiation within 25km": len(systems_all_init25.groupby("geom_name")),
        "Initiation within 10km": len(systems_all_init10.groupby("geom_name")),
    },
    index=["Convective Systems (total)"],
)
classes.loc["CS spontaneously generated (%)"] = [
    len(
        systems_all_init25.loc[
            (systems_all_init25.event == "SPONTANEOUS_GENERATION")
            & (~systems_all_init25.timestamp.isin(dts))
        ]
    )
    / classes["Initiation within 25km"][0]
    * 100,
    len(
        systems_all_init10.loc[
            (systems_all_init10.event == "SPONTANEOUS_GENERATION")
            & (~systems_all_init10.timestamp.isin(dts))
        ]
    )
    / classes["Initiation within 10km"][0]
    * 100,
]
classes.loc["CS with split/merge (%)"] = [
    len(
        systems_all_init25.loc[
            systems_all_init25.event.isin(["SPLIT", "MERGE"])
        ].groupby("geom_name")
    )
    / classes["Initiation within 25km"][0]
    * 100,
    len(
        systems_all_init10.loc[
            systems_all_init10.event.isin(["SPLIT", "MERGE"])
        ].groupby("geom_name")
    )
    / classes["Initiation within 10km"][0]
    * 100,
]
classes.loc["CS with full lifecycle (%)"] = [
    len(
        systems_all_init25.sort_values("timestamp")
        .groupby("geom_name")
        .tail(1)
        .set_index("event")
        .loc["CONTINUITY"]
    )
    / classes["Initiation within 25km"][0]
    * 100,
    len(
        systems_all_init10.sort_values("timestamp")
        .groupby("geom_name")
        .tail(1)
        .set_index("event")
        .loc["CONTINUITY"]
    )
    / classes["Initiation within 10km"][0]
    * 100,
]

fig, ax = plt.subplots(figsize=(6, 2))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

table = ax.table(
    cellText=classes.values.astype(int),
    colLabels=classes.columns,
    rowLabels=classes.index,
    loc="center",
    colWidths=[0.2, 0.2],
)
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))

fig.tight_layout()

plt.savefig(
    figpath + "exploratory_stats_class_init.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, ax = [None] * 2

# 4. Duration, all data
print("---- Plotting duration ----")
durs = pd.DataFrame(
    {
        "Raw": systems_per_full.groupby(
            [pd.cut(systems_per_full.duration, [0, 1, 2, 3, 4, 5, 6, 24])]
        ).size(),
        "Filtered": systems_per.groupby(
            [pd.cut(systems_per.duration, [0, 1, 2, 3, 4, 5, 6, 24])]
        ).size(),
    }
).append(
    {"Raw": str(len(systems_per_full)), "Filtered": str(len(systems_per))},
    ignore_index=True,
)
durs = durs.rename(
    index=dict(
        zip(
            durs.index.values,
            [
                "CS duration ≤ 1h",
                "1h < CS duration ≤ 2h",
                "2h < CS duration ≤ 3h",
                "3h < CS duration ≤ 4h",
                "4h < CS duration ≤ 5h",
                "5h < CS duration ≤ 6h",
                "CS duration > 6h",
                "Total",
            ],
        )
    )
)
norm = mpcolors.LogNorm(1, 100000)
colors = [
    [
        (
            "white"
            if not np.issubdtype(type(val), np.number)
            else plt.cm.BuPu(norm(val))
        )
        for val in row
    ]
    for row in durs.values
]

fig, ax = plt.subplots(figsize=(6, 2))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

table = ax.table(
    cellText=durs.values.astype(int),
    colLabels=durs.columns,
    cellColours=colors,
    rowLabels=durs.index,
    loc="center",
    colWidths=[0.2, 0.2],
)
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
    if (row > 0) & (col >= 0):
        cell.set_alpha(0.5)

fig.tight_layout()

plt.savefig(figpath + "exploratory_stats_dur.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, ax = [None] * 2

print("---- Plotting duration init ----")
durs = pd.DataFrame(
    {
        "Initiation within 25km": systems_per_init25.groupby(
            [pd.cut(systems_per_init25.duration, [0, 1, 2, 3, 4, 5, 6, 24])]
        ).size(),
        "Initiation within 10km": systems_per_init10.groupby(
            [pd.cut(systems_per_init10.duration, [0, 1, 2, 3, 4, 5, 6, 24])]
        ).size(),
    }
).append(
    {"Initiation within 25km": str(len(systems_per_init25)), "Initiation within 10km": str(len(systems_per_init10))},
    ignore_index=True,
)
durs = durs.rename(
    index=dict(
        zip(
            durs.index.values,
            [
                "CS duration ≤ 1h",
                "1h < CS duration ≤ 2h",
                "2h < CS duration ≤ 3h",
                "3h < CS duration ≤ 4h",
                "4h < CS duration ≤ 5h",
                "5h < CS duration ≤ 6h",
                "CS duration > 6h",
                "Total",
            ],
        )
    )
)
norm = mpcolors.LogNorm(1, 100000)
colors = [
    [
        (
            "white"
            if not np.issubdtype(type(val), np.number)
            else plt.cm.BuPu(norm(val))
        )
        for val in row
    ]
    for row in durs.values
]

fig, ax = plt.subplots(figsize=(6, 2))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

table = ax.table(
    cellText=durs.values.astype(int),
    colLabels=durs.columns,
    cellColours=colors,
    rowLabels=durs.index,
    loc="center",
    colWidths=[0.2, 0.2],
)
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
    if (row > 0) & (col >= 0):
        cell.set_alpha(0.5)

fig.tight_layout()

plt.savefig(figpath + "exploratory_stats_dur_init.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, ax = [None] * 2


# 5. Clusters, CS per month
print("---- Plotting clusters, CS per month ----")
monthly_all = pd.DataFrame(
    {
        "Raw": (
            systems_all_full.resample("1M", on="timestamp").count().timestamp
            / len(systems_all_full)
            * 100
        ),
        "Filtered": (
            systems_all.resample("1M", on="timestamp").count().timestamp
            / len(systems_all)
            * 100
        ),
    }
)
monthly_per = pd.DataFrame(
    {
        "Raw": (
            systems_per_full.resample("1M", on="timestamp").count().timestamp
            / len(systems_per_full)
            * 100
        ),
        "Filtered": (
            systems_per.resample("1M", on="timestamp").count().timestamp
            / len(systems_per)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = monthly_all.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax1)
axplot.set_ylim((0, 11))
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=8.5, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=8.5, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
# axplot.legend(
#     handles=legend_rfall_mon,
#     ncol=4,
#     fontsize="small",
#     loc="lower left",
#     mode="expand",
#     bbox_to_anchor=(0, 1.02, 1, 0.2),
#     frameon=True,
# )
axplot.legend(
    handles=legend_rfall_mon,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = monthly_per.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax2)
axplot.set_ylim((0, 11))
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=8.5, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=8.5, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Convective Systems")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_rfper_mon,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

fig.suptitle("Monthly distributions", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_monthly.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

print("---- Plotting clusters, CS per month init ----")
monthly_all = pd.DataFrame(
    {
        "Initiation within 25km": (
            systems_all_init25.resample("1M", on="timestamp").count().timestamp
            / len(systems_all_init25)
            * 100
        ),
        "Initiation within 10km": (
            systems_all_init10.resample("1M", on="timestamp").count().timestamp
            / len(systems_all_init10)
            * 100
        ),
    }
)
monthly_per = pd.DataFrame(
    {
        "Initiation within 25km": (
            systems_per_init25.resample("1M", on="timestamp").count().timestamp
            / len(systems_per_init25)
            * 100
        ),
        "Initiation within 10km": (
            systems_per_init10.resample("1M", on="timestamp").count().timestamp
            / len(systems_per_init10)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = monthly_all.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax1)
axplot.set_ylim((0, 11))
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=8.5, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=8.5, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_init)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
# axplot.legend(
#     handles=legend_rfall_mon,
#     ncol=4,
#     fontsize="small",
#     loc="lower left",
#     mode="expand",
#     bbox_to_anchor=(0, 1.02, 1, 0.2),
#     frameon=True,
# )
axplot.legend(
    handles=legend_rfall_init_mon,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = monthly_per.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax2)
axplot.set_ylim((0, 11))
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=8.5, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=8.5, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_init)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Convective Systems")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_rfper_init_mon,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

fig.suptitle("Monthly distributions", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_monthly_init.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5


# 6. Duration, per season
print("---- Plotting duration per season ----")
hourly_wet = pd.DataFrame(
    {
        "Raw": systems_per_full_wet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Filtered": systems_per_wet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Raw": str(len(systems_per_full_wet)),
        "Filtered": str(len(systems_per_wet)),
    },
    ignore_index=True,
)
hourly_dry = pd.DataFrame(
    {
        "Raw": systems_per_full_dry.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Filtered": systems_per_dry.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Raw": str(len(systems_per_full_dry)),
        "Filtered": str(len(systems_per_dry)),
    },
    ignore_index=True,
)
hourly_drytowet = pd.DataFrame(
    {
        "Raw": systems_per_full_drytowet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Filtered": systems_per_drytowet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Raw": str(len(systems_per_full_drytowet)),
        "Filtered": str(len(systems_per_drytowet)),
    },
    ignore_index=True,
)
hourly = pd.concat([hourly_dry, hourly_drytowet, hourly_wet], axis=1)
hourly = hourly.rename(
    index=dict(
        zip(
            hourly.index.values,
            [
                "CS duration ≤ 1h",
                "1h < CS duration ≤ 2h",
                "2h < CS duration ≤ 3h",
                "3h < CS duration ≤ 4h",
                "4h < CS duration ≤ 5h",
                "5h < CS duration ≤ 6h",
                "CS duration > 6h",
                "Total",
            ],
        )
    )
)
norm = mpcolors.LogNorm(1, 100000)
colors = [
    [
        (
            "white"
            if not np.issubdtype(type(val), np.number)
            else plt.cm.BuPu(norm(val))
        )
        for val in row
    ]
    for row in hourly.values
]

fig, ax = plt.subplots(figsize=(6, 3))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

header = ax.table(
    cellText=[[""] * 3],
    colLabels=["Dry Season", "Dry-to-Wet Season", "Wet Season"],
    loc="center",
    bbox=[0, 0.38, 1.0, 0.25],
)
table = ax.table(
    cellText=hourly.values.astype(int),
    colLabels=hourly.columns,
    cellColours=colors,
    rowLabels=hourly.index,
    loc="center",
    bbox=[0, -0.5, 1.0, 1.0],
    colWidths=[0.2, 0.2, 0.2, 0.2, 0.2, 0.2],
)
for (row, col), cell in header.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
    if (row > 0) & (col >= 0):
        cell.set_alpha(0.5)

fig.tight_layout()


plt.savefig(
    figpath + "exploratory_stats_dur_seasons.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, ax, header, table = [None] * 4

print("---- Plotting duration per season init ----")
hourly_wet = pd.DataFrame(
    {
        "Initiation within 25km": systems_per_init25_wet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Initiation within 10km": systems_per_init10_wet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Initiation within 25km": str(len(systems_per_init25_wet)),
        "Initiation within 10km": str(len(systems_per_init10_wet)),
    },
    ignore_index=True,
)
hourly_dry = pd.DataFrame(
    {
        "Initiation within 25km": systems_per_init25_dry.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Initiation within 10km": systems_per_init10_dry.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Initiation within 25km": str(len(systems_per_init25_dry)),
        "Initiation within 10km": str(len(systems_per_init10_dry)),
    },
    ignore_index=True,
)
hourly_drytowet = pd.DataFrame(
    {
        "Initiation within 25km": systems_per_init25_drytowet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Initiation within 10km": systems_per_init10_drytowet.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Initiation within 25km": str(len(systems_per_init25_drytowet)),
        "Initiation within 10km": str(len(systems_per_init10_drytowet)),
    },
    ignore_index=True,
)
hourly = pd.concat([hourly_dry, hourly_drytowet, hourly_wet], axis=1)
hourly = hourly.rename(
    index=dict(
        zip(
            hourly.index.values,
            [
                "CS duration ≤ 1h",
                "1h < CS duration ≤ 2h",
                "2h < CS duration ≤ 3h",
                "3h < CS duration ≤ 4h",
                "4h < CS duration ≤ 5h",
                "5h < CS duration ≤ 6h",
                "CS duration > 6h",
                "Total",
            ],
        )
    )
)
norm = mpcolors.LogNorm(1, 100000)
colors = [
    [
        (
            "white"
            if not np.issubdtype(type(val), np.number)
            else plt.cm.BuPu(norm(val))
        )
        for val in row
    ]
    for row in hourly.values
]

fig, ax = plt.subplots(figsize=(6, 3))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

header = ax.table(
    cellText=[[""] * 3],
    colLabels=["Dry Season", "Dry-to-Wet Season", "Wet Season"],
    loc="center",
    bbox=[0, 0.38, 1.0, 0.25],
)
table = ax.table(
    cellText=hourly.values.astype(int),
    colLabels=hourly.columns,
    cellColours=colors,
    rowLabels=hourly.index,
    loc="center",
    bbox=[0, -0.5, 1.0, 1.0],
    colWidths=[0.2, 0.2, 0.2, 0.2, 0.2, 0.2],
)
for (row, col), cell in header.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
    if (row > 0) & (col >= 0):
        cell.set_alpha(0.5)

fig.tight_layout()


plt.savefig(
    figpath + "exploratory_stats_dur_seasons_init.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, ax, header, table = [None] * 4


# 7. Duration, per IOP
print("---- Plotting duration per IOPs ----")
hourly_iop1 = pd.DataFrame(
    {
        "Raw": systems_per_full_iop1.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Filtered": systems_per_iop1.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Raw": str(len(systems_per_full_iop1)),
        "Filtered": str(len(systems_per_iop1)),
    },
    ignore_index=True,
)
hourly_iop2 = pd.DataFrame(
    {
        "Raw": systems_per_full_iop2.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Filtered": systems_per_iop2.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Raw": str(len(systems_per_full_iop2)),
        "Filtered": str(len(systems_per_iop2)),
    },
    ignore_index=True,
)
hourly = pd.concat([hourly_iop1, hourly_iop2], axis=1)
hourly = hourly.rename(
    index=dict(
        zip(
            hourly.index.values,
            [
                "CS duration ≤ 1h",
                "1h < CS duration ≤ 2h",
                "2h < CS duration ≤ 3h",
                "3h < CS duration ≤ 4h",
                "4h < CS duration ≤ 5h",
                "5h < CS duration ≤ 6h",
                "CS duration > 6h",
                "Total",
            ],
        )
    )
)
norm = mpcolors.LogNorm(1, 100000)
colors = [
    [
        (
            "white"
            if not np.issubdtype(type(val), np.number)
            else plt.cm.BuPu(norm(val))
        )
        for val in row
    ]
    for row in hourly.values
]

fig, ax = plt.subplots(figsize=(6, 3))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

header = ax.table(
    cellText=[[""] * 2],
    colLabels=["IOP1 (Wet Season)", "IOP2 (Dry Season)"],
    loc="center",
    bbox=[0, 0.385, 1.0, 0.25],
)
table = ax.table(
    cellText=hourly.values.astype(int),
    colLabels=hourly.columns,
    cellColours=colors,
    rowLabels=hourly.index,
    loc="center",
    bbox=[0, -0.5, 1.0, 1.0],
    colWidths=[0.2, 0.2, 0.2, 0.2],
)
for (row, col), cell in header.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
    if (row > 0) & (col >= 0):
        cell.set_alpha(0.5)

fig.tight_layout()


plt.savefig(
    figpath + "exploratory_stats_dur_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, ax, header, table = [None] * 4

print("---- Plotting duration per IOPs init ----")
hourly_iop1 = pd.DataFrame(
    {
        "Initiation within 25km": systems_per_init25_iop1.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Initiation within 10km": systems_per_init10_iop1.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Initiation within 25km": str(len(systems_per_init25_iop1)),
        "Initiation within 10km": str(len(systems_per_init10_iop1)),
    },
    ignore_index=True,
)
hourly_iop2 = pd.DataFrame(
    {
        "Initiation within 25km": systems_per_init25_iop2.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
        "Initiation within 10km": systems_per_init10_iop2.duration.value_counts(
            bins=[0, 1, 2, 3, 4, 5, 6, 24]
        ),
    }
).append(
    {
        "Initiation within 25km": str(len(systems_per_init25_iop2)),
        "Initiation within 10km": str(len(systems_per_init10_iop2)),
    },
    ignore_index=True,
)
hourly = pd.concat([hourly_iop1, hourly_iop2], axis=1)
hourly = hourly.rename(
    index=dict(
        zip(
            hourly.index.values,
            [
                "CS duration ≤ 1h",
                "1h < CS duration ≤ 2h",
                "2h < CS duration ≤ 3h",
                "3h < CS duration ≤ 4h",
                "4h < CS duration ≤ 5h",
                "5h < CS duration ≤ 6h",
                "CS duration > 6h",
                "Total",
            ],
        )
    )
)
norm = mpcolors.LogNorm(1, 100000)
colors = [
    [
        (
            "white"
            if not np.issubdtype(type(val), np.number)
            else plt.cm.BuPu(norm(val))
        )
        for val in row
    ]
    for row in hourly.values
]

fig, ax = plt.subplots(figsize=(6, 3))

# hide axes
fig.patch.set_visible(False)
ax.axis("off")
ax.axis("tight")

header = ax.table(
    cellText=[[""] * 2],
    colLabels=["IOP1 (Wet Season)", "IOP2 (Dry Season)"],
    loc="center",
    bbox=[0, 0.385, 1.0, 0.25],
)
table = ax.table(
    cellText=hourly.values.astype(int),
    colLabels=hourly.columns,
    cellColours=colors,
    rowLabels=hourly.index,
    loc="center",
    bbox=[0, -0.5, 1.0, 1.0],
    colWidths=[0.2, 0.2, 0.2, 0.2],
)
for (row, col), cell in header.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
for (row, col), cell in table.get_celld().items():
    if row == 0:
        cell.set_text_props(fontproperties=FontProperties(weight="bold"))
    if (row > 0) & (col >= 0):
        cell.set_alpha(0.5)

fig.tight_layout()


plt.savefig(
    figpath + "exploratory_stats_dur_iops_init.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, ax, header, table = [None] * 4


# 8. Clusters during the day, per season
print("---- Plotting clusters during the day per season ----")
hourly_wet = pd.DataFrame(
    {
        "full": (
            systems_all_full_wet.groupby(systems_all_full_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_full_wet)
            * 100
        ),
        "no-full": (
            systems_all_wet.groupby(systems_all_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_wet)
            * 100
        ),
    }
)
hourly_dry = pd.DataFrame(
    {
        "full": (
            systems_all_full_dry.groupby(systems_all_full_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_full_dry)
            * 100
        ),
        "no-full": (
            systems_all_dry.groupby(systems_all_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_dry)
            * 100
        ),
    }
)
hourly_drytowet = pd.DataFrame(
    {
        "full": (
            systems_all_full_drytowet.groupby(
                systems_all_full_drytowet.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_all_full_drytowet)
            * 100
        ),
        "no-full": (
            systems_all_drytowet.groupby(systems_all_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_drytowet)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_dry.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax1)
axplot.set_ylim((0, 18))
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry Season")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_dry, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_drytowet.plot(
    kind="bar", color=["w", "k"], edgecolor="k", ax=ax2
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry-to-Wet Season")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_drytowet, loc="upper left")

ax3 = fig.add_subplot(gs[2, 0])
axplot = hourly_wet.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax3)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Wet Season")
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_wet, loc="upper left")

fig.suptitle("Hourly distribution of clusters", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(figpath + "exploratory_stats_hourly.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, axplot = [None] * 6

print("---- Plotting clusters during the day per season init ----")
hourly_wet = pd.concat(
    [
        pd.DataFrame(index=pd.Index(np.arange(24))),
        pd.DataFrame(
            {
                "init25": (
                    systems_all_init25_wet.groupby(
                        systems_all_init25_wet.timestamp.dt.hour
                    )
                    .count()
                    .timestamp
                    / len(systems_all_init25_wet)
                    * 100
                ),
                "init10": (
                    systems_all_init10_wet.groupby(
                        systems_all_init10_wet.timestamp.dt.hour
                    )
                    .count()
                    .timestamp
                    / len(systems_all_init10_wet)
                    * 100
                ),
            }
        ),
    ],
    axis=1,
)
hourly_dry = pd.concat(
    [
        pd.DataFrame(index=pd.Index(np.arange(24))),
        pd.DataFrame(
            {
                "init25": (
                    systems_all_init25_dry.groupby(
                        systems_all_init25_dry.timestamp.dt.hour
                    )
                    .count()
                    .timestamp
                    / len(systems_all_init25_dry)
                    * 100
                ),
                "init10": (
                    systems_all_init10_dry.groupby(
                        systems_all_init10_dry.timestamp.dt.hour
                    )
                    .count()
                    .timestamp
                    / len(systems_all_init10_dry)
                    * 100
                ),
            }
        ),
    ],
    axis=1,
)
hourly_drytowet = pd.concat(
    [
        pd.DataFrame(index=pd.Index(np.arange(24))),
        pd.DataFrame(
            {
                "init25": (
                    systems_all_full_drytowet.groupby(
                        systems_all_full_drytowet.timestamp.dt.hour
                    )
                    .count()
                    .timestamp
                    / len(systems_all_full_drytowet)
                    * 100
                ),
                "init10": (
                    systems_all_init10_drytowet.groupby(
                        systems_all_init10_drytowet.timestamp.dt.hour
                    )
                    .count()
                    .timestamp
                    / len(systems_all_init10_drytowet)
                    * 100
                ),
            }
        ),
    ],
    axis=1,
)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_dry.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax1)
axplot.set_ylim((0, 18))
axplot.set_xlim((0, 23))
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry Season")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_init_dry, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_drytowet.plot(
    kind="bar", color=["g", "b"], edgecolor="k", ax=ax2
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry-to-Wet Season")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_init_drytowet, loc="upper left")

ax3 = fig.add_subplot(gs[2, 0])
axplot = hourly_wet.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax3)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Wet Season")
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_init_wet, loc="upper left")

fig.suptitle("Hourly distribution of clusters", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_hourly_init.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, axplot = [None] * 6


# 9. Clusters during the day, per IOP
print("---- Plotting clusters during the day per IOP ----")

hourly_iop1 = pd.DataFrame(
    {
        "full": (
            systems_all_full_iop1.groupby(
                systems_all_full_iop1.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_all_full_iop1)
            * 100
        ),
        "no-full": (
            systems_all_iop1.groupby(systems_all_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_iop1)
            * 100
        ),
    }
)
hourly_ìop2 = pd.DataFrame(
    {
        "full": (
            systems_all_full_iop2.groupby(
                systems_all_full_iop2.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_all_full_iop2)
            * 100
        ),
        "no-full": (
            systems_all_iop2.groupby(systems_all_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_iop2)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_iop1.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax1)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 17))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("IOP1 (Wet Season)")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_iop1, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_ìop2.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax2)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 17))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("IOP2 (Dry Season)")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_iop2, loc="upper left")

fig.suptitle("Hourly distribution of clusters", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_hourly_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

print("---- Plotting clusters during the day per IOP init ----")

hourly_iop1 = pd.concat([pd.DataFrame(index=pd.Index(np.arange(24))),pd.DataFrame(
    {
        "init25": (
            systems_all_init25_iop1.groupby(
                systems_all_init25_iop1.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_all_init25_iop1)
            * 100
        ),
        "init10": (
            systems_all_init10_iop1.groupby(systems_all_init10_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_init10_iop1)
            * 100
        ),
    }
)], axis=1)
hourly_ìop2 = pd.concat([pd.DataFrame(index=pd.Index(np.arange(24))),pd.DataFrame(
    {
        "init25": (
            systems_all_init25_iop2.groupby(
                systems_all_init25_iop2.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_all_init25_iop2)
            * 100
        ),
        "init10": (
            systems_all_init10_iop2.groupby(systems_all_init10_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_init10_iop2)
            * 100
        ),
    }
)], axis=1)

fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_iop1.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax1)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 17))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("IOP1 (Wet Season)")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_init_iop1, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_ìop2.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax2)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 17))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("IOP2 (Dry Season)")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfall_init_iop2, loc="upper left")

fig.suptitle("Hourly distribution of clusters", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_hourly_iops_init.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5


# 10. Init time during the day, per season
print("---- Plotting initiation during the day per season ----")

hourly_wet = pd.DataFrame(
    {
        "full": (
            systems_per_full_wet.groupby(systems_per_full_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_full_wet)
            * 100
        ),
        "no-full": (
            systems_per_wet.groupby(systems_per_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_wet)
            * 100
        ),
    }
)
hourly_dry = pd.DataFrame(
    {
        "full": (
            systems_per_full_dry.groupby(systems_per_full_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_full_dry)
            * 100
        ),
        "no-full": (
            systems_per_dry.groupby(systems_per_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_dry)
            * 100
        ),
    }
)
hourly_drytowet = pd.DataFrame(
    {
        "full": (
            systems_per_full_drytowet.groupby(
                systems_per_full_drytowet.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_per_full_drytowet)
            * 100
        ),
        "no-full": (
            systems_per_drytowet.groupby(systems_per_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_drytowet)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_dry.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax1)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry Season")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_dry, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_drytowet.plot(
    kind="bar", color=["w", "k"], edgecolor="k", ax=ax2
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry-to-Wet Season")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_drytowet, loc="upper left")

ax3 = fig.add_subplot(gs[2, 0])
axplot = hourly_wet.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax3)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Wet Season")
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_wet, loc="upper left")

fig.suptitle("Convective systems initiation time", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(figpath + "exploratory_stats_init.png", dpi=300, facecolor="none")

plt.clf()
fig, gs, ax1, ax2, ax3, axplot = [None] * 6

print("---- Plotting initiation during the day per season init ----")

hourly_wet = pd.concat(
    [
        pd.DataFrame(index=pd.Index(np.arange(24))),
        pd.DataFrame(
    {
        "init25": (
            systems_per_init25_wet.groupby(systems_per_init25_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_init25_wet)
            * 100
        ),
        "init10": (
            systems_per_init10_wet.groupby(systems_per_init10_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_init10_wet)
            * 100
        ),
    }
)], axis=1)
hourly_dry =  pd.concat(
    [
        pd.DataFrame(index=pd.Index(np.arange(24))),
        pd.DataFrame(
    {
        "init25": (
            systems_per_init25_dry.groupby(systems_per_init25_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_init25_dry)
            * 100
        ),
        "init10": (
            systems_per_init10_dry.groupby(systems_per_init10_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_init10_dry)
            * 100
        ),
    }
)], axis=1)
hourly_drytowet =  pd.concat(
    [
        pd.DataFrame(index=pd.Index(np.arange(24))),
        pd.DataFrame(
    {
        "init25": (
            systems_per_init25_drytowet.groupby(
                systems_per_init25_drytowet.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_per_init25_drytowet)
            * 100
        ),
        "init10": (
            systems_per_init10_drytowet.groupby(systems_per_init10_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_init10_drytowet)
            * 100
        ),
    }
)], axis=1)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_dry.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax1)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry Season")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_init_dry, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_drytowet.plot(
    kind="bar", color=["g", "b"], edgecolor="k", ax=ax2
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Dry-to-Wet Season")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_init_drytowet, loc="upper left")

ax3 = fig.add_subplot(gs[2, 0])
axplot = hourly_wet.plot(kind="bar", color=["g", "b"], edgecolor="k", ax=ax3)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 18))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Wet Season")
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_init_wet, loc="upper left")

fig.suptitle("Convective systems initiation time", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(figpath + "exploratory_stats_init_init.png", dpi=300, facecolor="none")

plt.clf()
fig, gs, ax1, ax2, ax3, axplot = [None] * 6


# 11. Init time during the day, per IOP
print("---- Plotting initiation during the day per IOP ----")

hourly_iop1 = pd.DataFrame(
    {
        "full": (
            systems_per_full_iop1.groupby(
                systems_per_full_iop1.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_per_full_iop1)
            * 100
        ),
        "no-full": (
            systems_per_iop1.groupby(systems_per_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_iop1)
            * 100
        ),
    }
)
hourly_ìop2 = pd.DataFrame(
    {
        "full": (
            systems_per_full_iop2.groupby(
                systems_per_full_iop2.timestamp.dt.hour
            )
            .count()
            .timestamp
            / len(systems_per_full_iop2)
            * 100
        ),
        "no-full": (
            systems_per_iop2.groupby(systems_per_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_iop2)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_iop1.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax1)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 17))
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("IOP1 (Wet Season)")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_iop1, loc="upper left")

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_ìop2.plot(kind="bar", color=["w", "k"], edgecolor="k", ax=ax2)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.grid(axis="x")
axplot.set_ylim((0, 17))
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("IOP2 (Dry Season)")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(handles=legend_rfper_iop2, loc="upper left")

fig.suptitle("Convective systems initiation time", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_init_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5


# 12. Clusters, CS, GLD per month
print("---- Plotting clusters, CS, GLD per month ----")

monthlyc = pd.DataFrame(
    {
        "gld": (
            systems_all.loc[systems_all.gld > 0]
            .resample("1M", on="timestamp")
            .count()
            .timestamp
            / len(systems_all)
            * 100
        ),
        "no-gld": (
            systems_all.loc[systems_all.gld == 0]
            .resample("1M", on="timestamp")
            .count()
            .timestamp
            / len(systems_all)
            * 100
        ),
    }
)
monthlycs = pd.DataFrame(
    {
        "gld": (
            systems_all.loc[systems_all.gld > 0]
            .groupby("geom_name")
            .first()
            .resample("1M", on="timestamp")
            .count()
            .timestamp
            / len(systems_all.groupby("geom_name").first())
            * 100
        ),
        "no-gld": (
            systems_all.loc[systems_all.geom_name.isin(nogld_names)]
            .groupby("geom_name")
            .first()
            .resample("1M", on="timestamp")
            .count()
            .timestamp
            / len(systems_all.groupby("geom_name").first())
            * 100
        ),
    }
)
monthlyg = pd.DataFrame(
    {
        "no-full": (
            systems_all.resample("1M", on="timestamp").sum().gld
            / systems_all.gld.sum()
            * 100
        )
    }
)

fig = plt.figure(figsize=(7, 9))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = monthlyc.plot(
    kind="bar", color=["gold", "gray"], edgecolor="k", ax=ax1
)
axplot.set_ylim((0, 7))
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=5, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=5, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters (total = " + str(len(systems_all)) + ")")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_ngld_seasons,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = monthlycs.plot(
    kind="bar", color=["gold", "gray"], edgecolor="k", ax=ax2
)
axplot.set_ylim((0, 7))
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=5, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=5, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels)
axplot.set_ylabel("Frequency (%)")
axplot.set_title(
    "Convective Systems (total = "
    + str(len(systems_all.groupby("geom_name").first()))
    + ")"
)
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_ngldcs_seasons,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

ax3 = fig.add_subplot(gs[2, 0])
axplot = monthlyg.plot(kind="bar", color=["k"], edgecolor="k", ax=ax3)
# Wet/dry season bars
axplot.axvspan(
    -0.5, 2.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(6.5, 9.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
axplot.axvspan(
    11.5, 14.5, facecolor="dodgerblue", edgecolor="none", alpha=0.5, zorder=0
)
axplot.axvspan(18.5, 21.5, facecolor="r", edgecolor="none", alpha=0.5, zorder=0)
# IOPs lines/labels
axplot.axvline([0.5], color="k", linestyle="--")
axplot.axvline([2.5], color="k", linestyle="--")
axplot.axvline([7], color="k", linestyle="--")
axplot.axvline([9], color="k", linestyle="--")
axplot.text(x=1.5, y=15, s="IOP1", fontweight="bold", ha="center")
axplot.text(x=8, y=15, s="IOP2", fontweight="bold", ha="center")
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels)
axplot.set_ylabel("Frequency (%)")
axplot.set_title(
    "GLD strokes (total = " + str(int(systems_all.gld.sum())) + ")"
)
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_seasons,
    ncol=4,
    fontsize="small",
    loc="upper center",
    bbox_to_anchor=(0.5, 1),
    labelspacing=0.25,
    columnspacing=1,
    frameon=True,
    framealpha=1,
)

fig.suptitle("Monthly distributions", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_gld_c_monthly.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, axplot = [None] * 6


# 12. Clusters, initiation, GLD per season
print("---- Plotting clusters, initiation, GLD per season ----")

# - VERSION WITH LIGHTNING
hourly_cl = pd.DataFrame(
    {
        "Dry": (
            systems_all_dry.loc[systems_all_dry.gld > 0]
            .groupby(systems_all_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_dry.loc[systems_all_dry.gld > 0])
            * 100
        ),
        "Dry-to-Wet": (
            systems_all_drytowet.loc[systems_all_drytowet.gld > 0]
            .groupby(systems_all_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_drytowet.loc[systems_all_drytowet.gld > 0])
            * 100
        ),
        "Wet": (
            systems_all_wet.loc[systems_all_wet.gld > 0]
            .groupby(systems_all_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_wet.loc[systems_all_wet.gld > 0])
            * 100
        ),
    }
)
namesdry_gld = (
    systems_all_dry.loc[systems_all_dry.gld > 0]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
namesdrytowet_gld = (
    systems_all_drytowet.loc[systems_all_drytowet.gld > 0]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
nameswet_gld = (
    systems_all_wet.loc[systems_all_wet.gld > 0]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
hourly_init = pd.DataFrame(
    {
        "Dry": (
            systems_per_dry.loc[systems_per_dry.name.isin(namesdry_gld)]
            .groupby(systems_per_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_dry.loc[systems_per_dry.name.isin(namesdry_gld)])
            * 100
        ),
        "Dry-to-Wet": (
            systems_per_drytowet.loc[
                systems_per_drytowet.name.isin(namesdrytowet_gld)
            ]
            .groupby(systems_per_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(
                systems_per_drytowet.loc[
                    systems_per_drytowet.name.isin(namesdrytowet_gld)
                ]
            )
            * 100
        ),
        "Wet": (
            systems_per_wet.loc[systems_per_wet.name.isin(nameswet_gld)]
            .groupby(systems_per_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_wet.loc[systems_per_wet.name.isin(nameswet_gld)])
            * 100
        ),
    }
)
hourly_gld = pd.DataFrame(
    {
        "Dry": (
            systems_all_dry.groupby(systems_all_dry.timestamp.dt.hour).sum().gld
            / systems_all_dry.gld.sum()
            * 100
        ),
        "Dry-to-Wet": (
            systems_all_drytowet.groupby(systems_all_drytowet.timestamp.dt.hour)
            .sum()
            .gld
            / systems_all_drytowet.gld.sum()
            * 100
        ),
        "Wet": (
            systems_all_wet.groupby(systems_all_wet.timestamp.dt.hour).sum().gld
            / systems_all_wet.gld.sum()
            * 100
        ),
    }
)


fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_cl.plot(
    kind="bar",
    color=["red", "yellowgreen", "dodgerblue"],
    edgecolor="k",
    ax=ax1,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 20))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters with Lightning")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_gldcl_seasons,
    fontsize="small",
    loc="upper left",
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_init.plot(
    kind="bar",
    color=["red", "yellowgreen", "dodgerblue"],
    edgecolor="k",
    ax=ax2,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 20))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Initiation time of CS with Lightning")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_gldinit_seasons,
    fontsize="small",
    loc="upper left",
)

ax3 = fig.add_subplot(gs[2, 0])
axplot = hourly_gld.plot(
    kind="bar",
    color=["red", "yellowgreen", "dodgerblue"],
    edgecolor="k",
    ax=ax3,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 20))
axplot.grid(axis="x")
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("GLD strokes")
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_gld_seasons,
    fontsize="small",
    loc="upper left",
)

fig.suptitle("Hourly distribution per Season", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_gld_c_seasons.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
(
    hourly_cs,
    namesdry_gld,
    namesdrytowet_gld,
    nameswet_gld,
    hourly_init,
    hourly_gld,
    fig,
    gs,
    ax1,
    ax2,
    ax3,
    axplot,
) = [None] * 12


# - VERSION WITH NO LIGHTNING
hourly_cl = pd.DataFrame(
    {
        "Dry": (
            systems_all_dry.loc[systems_all_dry.gld == 0]
            .groupby(systems_all_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_dry.loc[systems_all_dry.gld == 0])
            * 100
        ),
        "Dry-to-Wet": (
            systems_all_drytowet.loc[systems_all_drytowet.gld == 0]
            .groupby(systems_all_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_drytowet.loc[systems_all_drytowet.gld == 0])
            * 100
        ),
        "Wet": (
            systems_all_wet.loc[systems_all_wet.gld == 0]
            .groupby(systems_all_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_wet.loc[systems_all_wet.gld == 0])
            * 100
        ),
    }
)
namesdry_gld = (
    systems_all_dry.loc[systems_all_dry.geom_name.isin(nogld_names)]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
namesdrytowet_gld = (
    systems_all_drytowet.loc[systems_all_drytowet.geom_name.isin(nogld_names)]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
nameswet_gld = (
    systems_all_wet.loc[systems_all_wet.geom_name.isin(nogld_names)]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
hourly_init = pd.DataFrame(
    {
        "Dry": (
            systems_per_dry.loc[systems_per_dry.name.isin(namesdry_gld)]
            .groupby(systems_per_dry.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_dry.loc[systems_per_dry.name.isin(namesdry_gld)])
            * 100
        ),
        "Dry-to-Wet": (
            systems_per_drytowet.loc[
                systems_per_drytowet.name.isin(namesdrytowet_gld)
            ]
            .groupby(systems_per_drytowet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(
                systems_per_drytowet.loc[
                    systems_per_drytowet.name.isin(namesdrytowet_gld)
                ]
            )
            * 100
        ),
        "Wet": (
            systems_per_wet.loc[systems_per_wet.name.isin(nameswet_gld)]
            .groupby(systems_per_wet.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_per_wet.loc[systems_per_wet.name.isin(nameswet_gld)])
            * 100
        ),
    }
)


fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_cl.plot(
    kind="bar",
    color=["red", "yellowgreen", "dodgerblue"],
    edgecolor="k",
    ax=ax1,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 23))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters without Lightning")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_nogldcl_seasons,
    fontsize="small",
    loc="upper left",
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_init.plot(
    kind="bar",
    color=["red", "yellowgreen", "dodgerblue"],
    edgecolor="k",
    ax=ax2,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 23))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_xlabel("Local Time")
axplot.set_title("Initiation time of CS without Lightning")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_nogldinit_seasons,
    fontsize="small",
    loc="upper left",
)

fig.suptitle("Hourly distribution per Season", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_nogld_c_seasons.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
(
    hourly_cs,
    namesdry_gld,
    namesdrytowet_gld,
    nameswet_gld,
    hourly_init,
    hourly_gld,
    fig,
    gs,
    ax1,
    ax2,
    axplot,
) = [None] * 11


# 13. Clusters, initiation, GLD per IOP
print("---- Plotting clusters, initiation, GLD per IOP ----")

# - VERSION WITH LIGHTNING
hourly_cl = pd.DataFrame(
    {
        "IOP1 (Wet Season)": (
            systems_all_iop1.loc[systems_all_iop1.gld > 0]
            .groupby(systems_all_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_iop1.loc[systems_all_iop1.gld > 0])
            * 100
        ),
        "IOP2 (Dry Season)": (
            systems_all_iop2.loc[systems_all_iop2.gld > 0]
            .groupby(systems_all_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_iop2.loc[systems_all_iop2.gld > 0])
            * 100
        ),
    }
)
namesiop1_gld = (
    systems_all_iop1.loc[systems_all_iop1.gld > 0]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
namesiop2_gld = (
    systems_all_iop2.loc[systems_all_iop2.gld > 0]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
hourly_init = pd.DataFrame(
    {
        "IOP1 (Wet Season)": (
            systems_per_iop1.loc[systems_per_iop1.name.isin(namesiop1_gld)]
            .groupby(systems_per_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(
                systems_per_iop1.loc[systems_per_iop1.name.isin(namesiop1_gld)]
            )
            * 100
        ),
        "IOP2 (Dry Season)": (
            systems_per_iop2.loc[systems_per_iop2.name.isin(namesiop2_gld)]
            .groupby(systems_per_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(
                systems_per_iop2.loc[systems_per_iop2.name.isin(namesiop2_gld)]
            )
            * 100
        ),
    }
)
hourly_init = hourly_init.reindex(index=list(hourly_cl.index))
hourly_gld = pd.DataFrame(
    {
        "IOP1 (Wet Season)": (
            systems_all_iop1.groupby(systems_all_iop1.timestamp.dt.hour)
            .sum()
            .gld
            / systems_all_iop1.gld.sum()
            * 100
        ),
        "IOP2 (Dry Season)": (
            systems_all_iop2.groupby(systems_all_iop2.timestamp.dt.hour)
            .sum()
            .gld
            / systems_all_iop2.gld.sum()
            * 100
        ),
    }
)


fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_cl.plot(
    kind="bar",
    color=["dodgerblue", "red"],
    edgecolor="k",
    ax=ax1,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 40))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters with Lightning")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_gldcl_iops,
    fontsize="small",
    loc="upper left",
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_init.plot(
    kind="bar",
    color=["dodgerblue", "red"],
    edgecolor="k",
    ax=ax2,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 40))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Initiation time of CS with Lightning")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_gldinit_iops,
    fontsize="small",
    loc="upper left",
)

ax3 = fig.add_subplot(gs[2, 0])
axplot = hourly_gld.plot(
    kind="bar",
    color=["dodgerblue", "red"],
    edgecolor="k",
    ax=ax3,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 40))
axplot.grid(axis="x")
axplot.set_xlabel("Local Time")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("GLD strokes")
axplot.set_title("c", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_gld_iops,
    fontsize="small",
    loc="upper left",
)

fig.suptitle("Hourly distribution per IOP", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_gld_c_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
(
    hourly_cs,
    namesiop1_gld,
    namesiop2_gld,
    hourly_init,
    hourly_gld,
    fig,
    gs,
    ax1,
    ax2,
    ax3,
    axplot,
) = [None] * 11


# - VERSION WITH NO LIGHTNING
hourly_cl = pd.DataFrame(
    {
        "IOP1 (Wet Season)": (
            systems_all_iop1.loc[systems_all_iop1.gld == 0]
            .groupby(systems_all_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_iop1.loc[systems_all_iop1.gld == 0])
            * 100
        ),
        "IOP2 (Dry Season)": (
            systems_all_iop2.loc[systems_all_iop2.gld == 0]
            .groupby(systems_all_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(systems_all_iop2.loc[systems_all_iop2.gld == 0])
            * 100
        ),
    }
)
namesiop1_gld = (
    systems_all_iop1.loc[systems_all_iop1.geom_name.isin(nogld_names)]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
namesiop2_gld = (
    systems_all_iop2.loc[systems_all_iop2.geom_name.isin(nogld_names)]
    .groupby("geom_name")
    .first()
    .index.tolist()
)
hourly_init = pd.DataFrame(
    {
        "IOP1 (Wet Season)": (
            systems_per_iop1.loc[systems_per_iop1.name.isin(namesiop1_gld)]
            .groupby(systems_per_iop1.timestamp.dt.hour)
            .count()
            .timestamp
            / len(
                systems_per_iop1.loc[systems_per_iop1.name.isin(namesiop1_gld)]
            )
            * 100
        ),
        "IOP2 (Dry Season)": (
            systems_per_iop2.loc[systems_per_iop2.name.isin(namesiop2_gld)]
            .groupby(systems_per_iop2.timestamp.dt.hour)
            .count()
            .timestamp
            / len(
                systems_per_iop2.loc[systems_per_iop2.name.isin(namesiop2_gld)]
            )
            * 100
        ),
    }
)
hourly_init = hourly_init.reindex(index=list(hourly_cl.index))


fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = hourly_cl.plot(
    kind="bar",
    color=["dodgerblue", "red"],
    edgecolor="k",
    ax=ax1,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 47))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_title("Clusters without Lightning")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_nogldcl_iops,
    fontsize="small",
    loc="upper left",
)

ax2 = fig.add_subplot(gs[1, 0])
axplot = hourly_init.plot(
    kind="bar",
    color=["dodgerblue", "red"],
    edgecolor="k",
    ax=ax2,
    legend=False,
)
# Day/night bars
axplot.axvspan(
    -0.5, 6, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.axvspan(6, 18, facecolor="yellow", edgecolor="none", alpha=0.1, zorder=0)
axplot.axvspan(
    18, 24, facecolor="midnightblue", edgecolor="none", alpha=0.1, zorder=0
)
axplot.set_ylim((0, 47))
axplot.grid(axis="x")
axplot.set_xlabel("")
axplot.set_xticklabels(labels=time_labels_hour)
axplot.set_ylabel("Frequency (%)")
axplot.set_xlabel("Local Time")
axplot.set_title("Initiation time of CS without Lightning")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(
    handles=legend_nogldinit_iops,
    fontsize="small",
    loc="upper left",
)

fig.suptitle("Hourly distribution per IOP", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_nogld_c_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
(
    hourly_cs,
    namesiop1_gld,
    namesiop2_gld,
    hourly_init,
    hourly_gld,
    fig,
    gs,
    ax1,
    ax2,
    axplot,
) = [None] * 10


# 14. CS initiation, propagation per month
print("---- Plotting CS initiation, propagation per month ----")

# - Treating systems_all as geodataframe, get centroid of each cluster geom
clusters_geom = gpd.GeoDataFrame(systems_all, geometry="geom").set_crs(
    "EPSG:3395"
)
clusters_geom["point"] = clusters_geom.geometry.to_crs(
    "EPSG:3857"
).centroid.to_crs("EPSG:3395")

# - Drawing linestring between centroid points in a per CS geodf
clusters_geomt = clusters_geom.groupby("geom_name").filter(lambda x: len(x) > 1)
linestrs = (
    clusters_geomt.groupby("geom_name")
    .point.apply(lambda x: LineString(x.tolist()))
    .rename("path")
)
systems_geom = gpd.GeoDataFrame(linestrs, geometry="path").set_crs("EPSG:3395")
systems_geom["timestamp"] = (
    clusters_geomt.groupby("geom_name").first().timestamp
)

# - Extracting first and last point from linestrings
systems_initend = systems_geom.rename(columns={"geom_name": "name"})
systems_initend["init"] = gpd.GeoSeries(None)
systems_initend["end"] = gpd.GeoSeries(None)

for index, row in systems_initend.iterrows():
    coords = [(coords) for coords in list(row["path"].coords)]
    first_coord, last_coord = [coords[i] for i in (0, -1)]
    systems_initend.at[index, "init"] = Point(first_coord)
    systems_initend.at[index, "end"] = Point(last_coord)

# - Extracting coordinates and calculating u, v, angle, distance
systems_initend["x"] = systems_initend.init.x
systems_initend["y"] = systems_initend.init.y
systems_initend["dx"] = systems_initend.end.x - systems_initend.init.x
systems_initend["dy"] = systems_initend.end.y - systems_initend.init.y
systems_initend["u"] = np.cos(
    np.arctan2(systems_initend.dy, systems_initend.dx)
)
systems_initend["v"] = np.sin(
    np.arctan2(systems_initend.dy, systems_initend.dx)
)
systems_initend["angle"] = (
    np.degrees(np.arctan2(systems_initend.dy, systems_initend.dx)) + 360
) % 360
systems_initend["distance"] = (
    (systems_initend.dx) ** 2 + (systems_initend.dy) ** 2
) ** (1 / 2)

# - and finally, plotting
crs = ccrs.PlateCarree()
fig = plt.figure(figsize=(10, 15))
gs = fig.add_gridspec(4, 3)

for month, name, flabel in zip(months, months_names, figlabels):
    mon_geom = systems_initend.loc[
        (systems_initend["timestamp"].dt.year == 2014)
        & (systems_initend["timestamp"].dt.month == month)
    ]
    ax = fig.add_subplot(gs[month - 1], projection=crs)
    axplot = mon_geom.to_crs(crs.proj4_init).init.plot(
        color="r", markersize=1, ax=ax, zorder=3
    )
    axqv = ax.quiver(
        mon_geom["x"],
        mon_geom["y"],
        mon_geom["distance"],
        mon_geom["distance"],
        angles=mon_geom["angle"],
        scale=10,
        headaxislength=4,
        headwidth=4,
        width=0.005,
        transform=crs,
        zorder=2,
    )
    axplot.set_xlim((-61.47, -58.5))
    axplot.set_ylim((-4.65, -1.45))  # (-4.65, -1.65)
    axplot.set_xlabel("Longitude (°)")
    axplot.set_ylabel("Latitude (°)")
    rivers.to_crs(crs.proj4_init).plot(alpha=0.5, ax=ax, zorder=1)
    ax.tissot(
        rad_km=150,
        lons=[
            -59.991,
        ],
        lats=[
            -3.149,
        ],
        n_samples=128,
        facecolor="none",
        edgecolor="gray",
        zorder=1,
    )
    gl = ax.gridlines(crs=crs, draw_labels=True, zorder=0)
    gl.top_labels = False
    gl.right_labels = False
    axplot.legend(handles=legend_pmap, loc="upper left", fontsize="small")
    ax.quiverkey(
        axqv,
        0.47,
        0.944,
        0.5,
        "Propagation direction",
        labelpos="E",
        coordinates="axes",
        fontproperties={"size": "small"},
    )
    axplot.set_title(name + " 2014 - " + str(len(mon_geom)) + " CSs")
    axplot.set_title(flabel, loc="left", fontweight="bold", size=16)

fig.suptitle(
    "Convective Systems Initiation and Propagation Direction",
    size=14,
    fontweight="bold",
)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_map_2014.png",
    dpi=300,
    facecolor="none",
)

plt.clf()
fig, gs, ax, axplot, axqv, gl = [None] * 6

crs = ccrs.PlateCarree()
fig = plt.figure(figsize=(10, 15))
gs = fig.add_gridspec(4, 3)

for month, name, flabel in zip(months, months_names, figlabels):
    mon_geom = systems_initend.loc[
        (systems_initend["timestamp"].dt.year == 2015)
        & (systems_initend["timestamp"].dt.month == month)
    ]
    ax = fig.add_subplot(gs[month - 1], projection=crs)
    axplot = mon_geom.to_crs(crs.proj4_init).init.plot(
        color="r", markersize=1, ax=ax, zorder=3
    )
    axqv = ax.quiver(
        mon_geom["x"],
        mon_geom["y"],
        mon_geom["distance"],
        mon_geom["distance"],
        angles=mon_geom["angle"],
        scale=10,
        headaxislength=4,
        headwidth=4,
        width=0.005,
        transform=crs,
        zorder=2,
    )
    axplot.set_xlim((-61.47, -58.5))
    axplot.set_ylim((-4.65, -1.45))  # (-4.65, -1.65)
    axplot.set_xlabel("Longitude (°)")
    axplot.set_ylabel("Latitude (°)")
    rivers.to_crs(crs.proj4_init).plot(alpha=0.5, ax=ax, zorder=1)
    ax.tissot(
        rad_km=150,
        lons=[
            -59.991,
        ],
        lats=[
            -3.149,
        ],
        n_samples=128,
        facecolor="none",
        edgecolor="gray",
        zorder=1,
    )
    gl = ax.gridlines(crs=crs, draw_labels=True, zorder=0)
    gl.top_labels = False
    gl.right_labels = False
    axplot.legend(handles=legend_pmap, loc="upper left", fontsize="small")
    ax.quiverkey(
        axqv,
        0.47,
        0.944,
        0.5,
        "Propagation direction",
        labelpos="E",
        coordinates="axes",
        fontproperties={"size": "small"},
    )
    axplot.set_title(name + " 2015 - " + str(len(mon_geom)) + " CSs")
    axplot.set_title(flabel, loc="left", fontweight="bold", size=16)

fig.suptitle(
    "Convective Systems Initiation and Propagation Direction",
    size=14,
    fontweight="bold",
)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_map_2015.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax, axplot, axqv, gl = [None] * 6


# 15. CS propagation: all, per season, IOPs
print("---- Plotting CS propagation: all, per season, IOPs ----")

propg = pd.DataFrame(
    {
        "angle_freq": (
            np.radians(systems_initend.angle).value_counts(
                bins=np.arange(0, 2.1 * np.pi, np.radians(22.5)), normalize=True
            )
            * 100
        )
    }
).sort_index()
propg.rename(
    index=dict(zip(propg.index.values, propg.index.left)), inplace=True
)

fig = plt.figure(figsize=(5, 5))
gs = fig.add_gridspec(1, 1)

ax = fig.add_subplot(gs[0], polar=True)
ax.bar(
    x=propg.index.values,
    height=propg.angle_freq,
    width=np.pi / 16,
    color="k",
    align="center",
)
ax.set_ylim((-2, 35))
ax.set_xticks(ticks=np.arange(0, 2 * np.pi, np.radians(22.5)), labels=geolabels)
ax.set_yticks(
    ticks=ax.get_yticks()[2:-1:2],
    labels=ax.get_yticklabels()[2:-1:2],
    ha="center",
)
ax.yaxis.set_major_formatter("{x:,.0f}%")
ax.set_title("Propagation Direction of Convective Systems", fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_propg_all.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2 = [None] * 4

propg_dry = pd.DataFrame(
    {
        "angle_freq": (
            np.radians(
                systems_initend.loc[
                    systems_initend["timestamp"].dt.month.isin([8, 9, 10])
                ].angle
            ).value_counts(
                bins=np.arange(0, 2.1 * np.pi, np.radians(22.5)), normalize=True
            )
            * 100
        )
    }
).sort_index()
propg_dry.rename(
    index=dict(zip(propg_dry.index.values, propg_dry.index.left)), inplace=True
)
propg_drytowet = pd.DataFrame(
    {
        "angle_freq": (
            np.radians(
                systems_initend.loc[
                    systems_initend["timestamp"].dt.month.isin([11, 12])
                ].angle
            ).value_counts(
                bins=np.arange(0, 2.1 * np.pi, np.radians(22.5)), normalize=True
            )
            * 100
        )
    }
).sort_index()
propg_drytowet.rename(
    index=dict(zip(propg_drytowet.index.values, propg_drytowet.index.left)),
    inplace=True,
)
propg_wet = pd.DataFrame(
    {
        "angle_freq": (
            np.radians(
                systems_initend.loc[
                    systems_initend["timestamp"].dt.month.isin([1, 2, 3])
                ].angle
            ).value_counts(
                bins=np.arange(0, 2.1 * np.pi, np.radians(22.5)), normalize=True
            )
            * 100
        )
    }
).sort_index()
propg_wet.rename(
    index=dict(zip(propg_wet.index.values, propg_wet.index.left)), inplace=True
)
propg_iop1 = pd.DataFrame(
    {
        "angle_freq": (
            np.radians(
                systems_initend.loc[
                    (systems_initend["timestamp"].dt.month.isin([2, 3]))
                    & (systems_initend["timestamp"].dt.year == 2014)
                ].angle
            ).value_counts(
                bins=np.arange(0, 2.1 * np.pi, np.radians(22.5)), normalize=True
            )
            * 100
        )
    }
).sort_index()
propg_iop1.rename(
    index=dict(zip(propg_iop1.index.values, propg_iop1.index.left)),
    inplace=True,
)
propg_iop2 = pd.DataFrame(
    {
        "angle_freq": (
            np.radians(
                systems_initend.set_index(["timestamp"])
                .loc["2014-8-15":"2014-10-15"]
                .reset_index()
                .angle
            ).value_counts(
                bins=np.arange(0, 2.1 * np.pi, np.radians(22.5)), normalize=True
            )
            * 100
        )
    }
).sort_index()
propg_iop2.rename(
    index=dict(zip(propg_iop2.index.values, propg_iop2.index.left)),
    inplace=True,
)


fig = plt.figure(figsize=(9, 4))
gs = fig.add_gridspec(1, 3)

ax1 = fig.add_subplot(gs[0], polar=True)
ax1.bar(
    x=propg_dry.index.values,
    height=propg_dry.angle_freq,
    width=np.pi / 16,
    color="k",
    align="center",
)
ax1.set_ylim((-2, 34))
ax1.set_xticks(
    ticks=np.arange(0, 2 * np.pi, np.radians(22.5)), labels=geolabels
)
ax1.set_yticks(
    ticks=ax1.get_yticks()[2:-1:2],
    labels=ax1.get_yticklabels()[2:-1:2],
    ha="center",
)
ax1.yaxis.set_major_formatter("{x:,.0f}%")
ax1.set_title(
    "Dry Season\n(total = "
    + str(
        len(
            systems_initend.loc[
                systems_initend["timestamp"].dt.month.isin([8, 9, 10])
            ]
        )
    )
    + ")"
)
ax1.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1], polar=True)
ax2.bar(
    x=propg_drytowet.index.values,
    height=propg_drytowet.angle_freq,
    width=np.pi / 16,
    color="k",
    align="center",
)
ax2.set_ylim((-2, 34))
ax2.set_xticks(
    ticks=np.arange(0, 2 * np.pi, np.radians(22.5)), labels=geolabels
)
ax2.set_yticks(
    ticks=ax2.get_yticks()[2:-1:2],
    labels=ax2.get_yticklabels()[2:-1:2],
    ha="center",
)
ax2.yaxis.set_major_formatter("{x:,.0f}%")
ax2.set_title(
    "Dry-to-Wet Season\n(total = "
    + str(
        len(
            systems_initend.loc[
                systems_initend["timestamp"].dt.month.isin([11, 12])
            ]
        )
    )
    + ")"
)
ax2.set_title("b", loc="left", fontweight="bold", size=16)

ax3 = fig.add_subplot(gs[2], polar=True)
ax3.bar(
    x=propg_wet.index.values,
    height=propg_wet.angle_freq,
    width=np.pi / 16,
    color="k",
    align="center",
)
ax3.set_ylim((-2, 34))
ax3.set_xticks(
    ticks=np.arange(0, 2 * np.pi, np.radians(22.5)), labels=geolabels
)
ax3.set_yticks(
    ticks=ax3.get_yticks()[2:-1:2],
    labels=ax3.get_yticklabels()[2:-1:2],
    ha="center",
)
ax3.yaxis.set_major_formatter("{x:,.0f}%")
ax3.set_title(
    "Wet Season\n(total = "
    + str(
        len(
            systems_initend.loc[
                systems_initend["timestamp"].dt.month.isin([1, 2, 3])
            ]
        )
    )
    + ")"
)
ax3.set_title("c", loc="left", fontweight="bold", size=16)

fig.suptitle(
    "Propagation Direction of Convective Systems", size=14, fontweight="bold"
)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_propg_seasons.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3 = [None] * 5

fig = plt.figure(figsize=(7, 4.5))
gs = fig.add_gridspec(1, 2)

ax1 = fig.add_subplot(gs[0], polar=True)
ax1.bar(
    x=propg_dry.index.values,
    height=propg_iop1.angle_freq,
    width=np.pi / 16,
    color="k",
    align="center",
)
ax1.set_ylim((-2, 40))
ax1.set_xticks(
    ticks=np.arange(0, 2 * np.pi, np.radians(22.5)), labels=geolabels
)
ax1.set_yticks(
    ticks=ax1.get_yticks()[2:-1:2],
    labels=ax1.get_yticklabels()[2:-1:2],
    ha="center",
)
ax1.yaxis.set_major_formatter("{x:,.0f}%")
ax1.set_title(
    "IOP1 (Wet Season)\n(total = "
    + str(
        len(
            systems_initend.loc[
                (systems_initend["timestamp"].dt.month.isin([2, 3]))
                & (systems_initend["timestamp"].dt.year == 2014)
            ]
        )
    )
    + ")"
)
ax1.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1], polar=True)
ax2.bar(
    x=propg_iop2.index.values,
    height=propg_iop2.angle_freq,
    width=np.pi / 16,
    color="k",
    align="center",
)
ax2.set_ylim((-2, 40))
ax2.set_xticks(
    ticks=np.arange(0, 2 * np.pi, np.radians(22.5)), labels=geolabels
)
ax2.set_yticks(
    ticks=ax2.get_yticks()[2:-1:2],
    labels=ax2.get_yticklabels()[2:-1:2],
    ha="center",
)
ax2.yaxis.set_major_formatter("{x:,.0f}%")
ax2.set_title(
    "IOP2 (Dry Season)\n(total = "
    + str(
        len(
            systems_initend.set_index(["timestamp"])
            .loc["2014-8-15":"2014-10-15"]
            .reset_index()
        )
    )
    + ")"
)
ax2.set_title("b", loc="left", fontweight="bold", size=16)

fig.suptitle(
    "Propagation Direction of Convective Systems", size=14, fontweight="bold"
)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_propg_iops.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2 = [None] * 4


# 16. Echo tops
print("---- Plotting echotops ----")
tops = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all.groupby("geom_name")
            .tail(1)
            .echotop_0.value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all.groupby("geom_name")
            .tail(1)
            .echotop_20.value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all.groupby("geom_name")
            .tail(1)
            .echotop_40.value_counts(normalize=True)
            * 100
        ),
    }
).drop(index=0)
detops = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all.dropna()
            .groupby("geom_name")
            .agg({"dechotop_0": lambda x: max(x, key=abs)})
            .value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all.dropna()
            .groupby("geom_name")
            .agg({"dechotop_20": lambda x: max(x, key=abs)})
            .value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all.dropna()
            .groupby("geom_name")
            .agg({"dechotop_40": lambda x: max(x, key=abs)})
            .value_counts(normalize=True)
            * 100
        ),
    }
)

fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = tops.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax1
)
axplot.grid(axis="x")
axplot.set_xlabel("Height (km)")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(ax1.get_xticks(), ax1.get_xticklabels(), rotation=0)
axplot.set_title("Convective Systems Max Echo Tops")
axplot.set_title("a", loc="left", fontweight="bold", size=16)
axplot.legend(ncol=3)

ax2 = fig.add_subplot(gs[1, 0])
axplot = detops.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax2
)
axplot.grid(axis="x")
axplot.set_xlabel("km/min")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(
    ax2.get_xticks(), np.round(np.arange(-1.1, 1.25, 0.1), 1), rotation=0
)
axplot.set_title("Convective Systems Max Echo Top Variation Rates")
axplot.set_title("b", loc="left", fontweight="bold", size=16)
axplot.legend(ncol=3)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_echotops.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5

# 16. Echo tops per season/IOP
print("---- Plotting echotops per season, IOP ----")
tops_wet = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all_wet.groupby("geom_name")
            .tail(1)
            .echotop_0.value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all_wet.groupby("geom_name")
            .tail(1)
            .echotop_20.value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all_wet.groupby("geom_name")
            .tail(1)
            .echotop_40.value_counts(normalize=True)
            * 100
        ),
    }
).drop(index=0)
tops_dry = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all_dry.groupby("geom_name")
            .tail(1)
            .echotop_0.value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all_dry.groupby("geom_name")
            .tail(1)
            .echotop_20.value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all_dry.groupby("geom_name")
            .tail(1)
            .echotop_40.value_counts(normalize=True)
            * 100
        ),
    }
).drop(index=0)
tops_drytowet = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all_drytowet.groupby("geom_name")
            .tail(1)
            .echotop_0.value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all_drytowet.groupby("geom_name")
            .tail(1)
            .echotop_20.value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all_drytowet.groupby("geom_name")
            .tail(1)
            .echotop_40.value_counts(normalize=True)
            * 100
        ),
    }
).drop(index=0)

fig = plt.figure(figsize=(7, 7))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = tops_dry.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax1
)
axplot.set_ylim((0, 24))
axplot.grid(axis="x")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(ax1.get_xticks(), ax1.get_xticklabels(), rotation=0)
axplot.set_title("Dry Season")
axplot.legend(loc="upper left")
# axplot.legend(handles=custom_legend_dry, loc='upper left')
axplot.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1, 0])
axplot = tops_drytowet.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax2
)
axplot.set_ylim((0, 24))
axplot.grid(axis="x")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(ax2.get_xticks(), ax2.get_xticklabels(), rotation=0)
axplot.set_title("Dry-to-Wet Season")
axplot.legend(loc="upper left")
# axplot.legend(handles=custom_legend_drytowet, loc='upper left')
axplot.set_title("b", loc="left", fontweight="bold", size=16)

ax3 = fig.add_subplot(gs[2, 0])
axplot = tops_wet.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax3
)
axplot.set_ylim((0, 24))
axplot.grid(axis="x")
axplot.set_xlabel("Height (km)")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(ax3.get_xticks(), ax3.get_xticklabels(), rotation=0)
axplot.set_title("Wet Season")
axplot.legend(loc="upper left")
# axplot.legend(handles=custom_legend_wet, loc='upper left')
axplot.set_title("c", loc="left", fontweight="bold", size=16)

fig.suptitle("Convective Systems Max Echo Tops", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_echotops_seasons.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, axplot = [None] * 6

tops_iop1 = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all_iop1.groupby("geom_name")
            .tail(1)
            .echotop_0.value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all_iop1.groupby("geom_name")
            .tail(1)
            .echotop_20.value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all_iop1.groupby("geom_name")
            .tail(1)
            .echotop_40.value_counts(normalize=True)
            * 100
        ),
    }
).drop(index=0)
tops_iop2 = pd.DataFrame(
    {
        "0 dBZ": (
            systems_all_iop2.groupby("geom_name")
            .tail(1)
            .echotop_0.value_counts(normalize=True)
            * 100
        ),
        "20 dBZ": (
            systems_all_iop2.groupby("geom_name")
            .tail(1)
            .echotop_20.value_counts(normalize=True)
            * 100
        ),
        "40 dBZ": (
            systems_all_iop2.groupby("geom_name")
            .tail(1)
            .echotop_40.value_counts(normalize=True)
            * 100
        ),
    }
).drop(index=0)

fig = plt.figure(figsize=(7, 5))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = tops_iop1.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax1
)
axplot.set_ylim((0, 28))
axplot.grid(axis="x")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(ax1.get_xticks(), ax1.get_xticklabels(), rotation=0)
axplot.set_title("IOP1 (Wet Season)")
axplot.legend(loc="upper left")
axplot.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1, 0])
axplot = tops_iop2.plot(
    kind="bar", color=("#E2E2E2", "#A1A6C8", "#023FA5"), edgecolor="k", ax=ax2
)
axplot.set_ylim((0, 28))
axplot.grid(axis="x")
axplot.set_xlabel("Height (km)")
axplot.set_ylabel("Frequency (%)")
axplot.set_xticks(ax2.get_xticks(), ax2.get_xticklabels(), rotation=0)
axplot.set_title("IOP2 (Dry Season)")
axplot.legend(loc="upper left")
axplot.set_title("b", loc="left", fontweight="bold", size=16)

fig.suptitle("Convective Systems Max Echo Tops", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_echotops_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, axplot = [None] * 5


# 17. CFADs
print("---- Plotting CFADs ----")
cfad = np.sum(zfreq, axis=0)

fig = plt.figure(figsize=(5, 4))
gs = fig.add_gridspec(1, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = plt.contourf(
    cfad / cfad.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_xlabel("Reflectivity (dBZ)")
ax1.set_ylabel("Height (km)")
ax1.set_title("Clusters CFAD", fontweight="bold")
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad / cfad.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

gs.tight_layout(fig)

plt.savefig(figpath + "exploratory_stats_cfad.png", dpi=300, facecolor="none")

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, cbar = [None] * 4


# 18. CFAD per season/IOP and differences
print("---- Plotting CFAD per season/IOP and differences ----")
cfad_wet = np.sum([zfreq[i] for i in systems_all_wet.index.to_list()], axis=0)
cfad_dry = np.sum([zfreq[i] for i in systems_all_dry.index.to_list()], axis=0)
cfad_drytowet = np.sum(
    [zfreq[i] for i in systems_all_drytowet.index.to_list()], axis=0
)
cfad_iop1 = np.sum([zfreq[i] for i in systems_all_iop1.index.to_list()], axis=0)
cfad_iop2 = np.sum([zfreq[i] for i in systems_all_iop2.index.to_list()], axis=0)
cfad_dry_wet = (cfad_dry / cfad_dry.sum(axis=1, keepdims=True) * 100) - (
    cfad_wet / cfad_wet.sum(axis=1, keepdims=True) * 100
)
cfad_drytowet_wet = (
    cfad_drytowet / cfad_drytowet.sum(axis=1, keepdims=True) * 100
) - (cfad_wet / cfad_wet.sum(axis=1, keepdims=True) * 100)
cfad_drytowet_dry = (
    cfad_drytowet / cfad_drytowet.sum(axis=1, keepdims=True) * 100
) - (cfad_dry / cfad_dry.sum(axis=1, keepdims=True) * 100)
cfad_iop1_iop2 = (cfad_iop1 / cfad_iop1.sum(axis=1, keepdims=True) * 100) - (
    cfad_iop2 / cfad_iop2.sum(axis=1, keepdims=True) * 100
)

fig = plt.figure(figsize=(9, 10))
gs = fig.add_gridspec(3, 2)

ax1 = fig.add_subplot(gs[0, 0])
plt.contourf(
    cfad_dry / cfad_dry.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_ylabel("Height (km)")
ax1.set_title("Dry Season")
ax1.set_title("a", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_dry / cfad_dry.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax2 = fig.add_subplot(gs[1, 0])
plt.contourf(
    cfad_drytowet / cfad_drytowet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax2.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax2.set_yticks(ax2.get_yticks(), range(2, 17, 2))
ax2.set_ylabel("Height (km)")
ax2.set_title("Dry-to-Wet Season")
ax2.set_title("b", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_drytowet / cfad_drytowet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax3 = fig.add_subplot(gs[2, 0])
plt.contourf(
    cfad_wet / cfad_wet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax3.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax3.set_yticks(ax3.get_yticks(), range(2, 17, 2))
ax3.set_xlabel("Reflectivity (dBZ)")
ax3.set_ylabel("Height (km)")
ax3.set_title("Wet Season")
ax3.set_title("c", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_wet / cfad_wet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax4 = fig.add_subplot(gs[1, 1])
plt.contourf(
    cfad_dry_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax4.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax4.set_yticks(ax4.get_yticks(), range(2, 17, 2))
ax4.set_ylabel("Height (km)")
ax4.set_title("Dry - Wet Season")
ax4.set_title("e", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_dry_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

ax5 = fig.add_subplot(gs[2, 1])
plt.contourf(
    cfad_drytowet_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax5.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax5.set_yticks(ax5.get_yticks(), range(2, 17, 2))
ax5.set_ylabel("Height (km)")
ax5.set_title("Dry-to-Wet - Wet Season")
ax5.set_title("f", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_drytowet_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

ax6 = fig.add_subplot(gs[0, 1])
plt.contourf(
    cfad_drytowet_dry,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax6.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax6.set_yticks(ax6.get_yticks(), range(2, 17, 2))
ax6.set_xlabel("Reflectivity (dBZ)")
ax6.set_ylabel("Height (km)")
ax6.set_title("Dry-to-Wet - Dry Season")
ax6.set_title("d", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_drytowet_dry,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

fig.suptitle("Clusters CFADs", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_cfad_seasons.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, ax4, ax5, ax6, cbar = [None] * 9


fig = plt.figure(figsize=(8, 7))
gs = fig.add_gridspec(4, 2)

ax1 = fig.add_subplot(gs[0:2, 0])
plt.contourf(
    cfad_iop1 / cfad_iop1.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_ylabel("Height (km)")
ax1.set_title("IOP1 (Wet Season)")
ax1.set_title("a", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_iop1 / cfad_iop1.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax2 = fig.add_subplot(gs[2:4, 0])
plt.contourf(
    cfad_iop2 / cfad_iop2.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax2.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax2.set_yticks(ax2.get_yticks(), range(2, 17, 2))
ax2.set_ylabel("Height (km)")
ax2.set_xlabel("Reflectivity (dBZ)")
ax2.set_title("IOP2 (Dry Season)")
ax2.set_title("b", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_iop2 / cfad_iop2.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax3 = fig.add_subplot(gs[1:3, 1])
axplot = plt.contourf(
    cfad_iop1_iop2,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax3.set_aspect("auto")
ax3.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax3.set_yticks(ax3.get_yticks(), range(2, 17, 2))
ax3.set_xlabel("Reflectivity (dBZ)")
ax3.set_ylabel("Height (km)")
ax3.set_title("IOP1 - IOP2")
ax3.set_title("c", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_iop1_iop2,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

fig.suptitle("Clusters CFADs", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_cfad_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, cbar = [None] * 6


fig = plt.figure(figsize=(5, 10))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
plt.contourf(
    cfad_dry / cfad_dry.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_ylabel("Height (km)")
ax1.set_title("Dry Season")
ax1.set_title("a", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_dry / cfad_dry.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax2 = fig.add_subplot(gs[1, 0])
plt.contourf(
    cfad_drytowet / cfad_drytowet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax2.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax2.set_yticks(ax2.get_yticks(), range(2, 17, 2))
ax2.set_ylabel("Height (km)")
ax2.set_title("Dry-to-Wet Season")
ax2.set_title("b", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_drytowet / cfad_drytowet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax3 = fig.add_subplot(gs[2, 0])
plt.contourf(
    cfad_wet / cfad_wet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax3.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax3.set_yticks(ax3.get_yticks(), range(2, 17, 2))
ax3.set_xlabel("Reflectivity (dBZ)")
ax3.set_ylabel("Height (km)")
ax3.set_title("Wet Season")
ax3.set_title("c", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_wet / cfad_wet.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

fig.suptitle("Clusters CFADs", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_cfad_seasons.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, cbar = [None] * 6


fig = plt.figure(figsize=(5, 7))
gs = fig.add_gridspec(2, 1)

ax1 = fig.add_subplot(gs[0, 0])
plt.contourf(
    cfad_iop1 / cfad_iop1.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_ylabel("Height (km)")
ax1.set_title("IOP1 (Wet Season)")
ax1.set_title("a", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_iop1 / cfad_iop1.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

ax2 = fig.add_subplot(gs[1, 0])
plt.contourf(
    cfad_iop2 / cfad_iop2.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    cmap="BuPu",
    zorder=0,
)
ax2.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax2.set_yticks(ax2.get_yticks(), range(2, 17, 2))
ax2.set_ylabel("Height (km)")
ax2.set_xlabel("Reflectivity (dBZ)")
ax2.set_title("IOP2 (Dry Season)")
ax2.set_title("b", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_iop2 / cfad_iop2.sum(axis=1, keepdims=True) * 100,
    extend="both",
    levels=np.arange(5, 60, 5),
    colors="k",
    linewidths=0.5,
)

fig.suptitle("Clusters CFADs", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_cfad_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, cbar = [None] * 6

fig = plt.figure(figsize=(5, 10))
gs = fig.add_gridspec(3, 1)

ax1 = fig.add_subplot(gs[0, 0])
plt.contourf(
    cfad_dry_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_ylabel("Height (km)")
ax1.set_title("Dry - Wet Season")
ax1.set_title("a", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_dry_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

ax2 = fig.add_subplot(gs[1, 0])
plt.contourf(
    cfad_drytowet_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax2.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax2.set_yticks(ax2.get_yticks(), range(2, 17, 2))
ax2.set_ylabel("Height (km)")
ax2.set_title("Dry-to-Wet - Wet Season")
ax2.set_title("b", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_drytowet_wet,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

ax3 = fig.add_subplot(gs[2, 0])
plt.contourf(
    cfad_drytowet_dry,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax3.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax3.set_yticks(ax3.get_yticks(), range(2, 17, 2))
ax3.set_xlabel("Reflectivity (dBZ)")
ax3.set_ylabel("Height (km)")
ax3.set_title("Dry-to-Wet - Dry Season")
ax3.set_title("c", loc="left", fontweight="bold", size=16)
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_drytowet_dry,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

fig.suptitle("Clusters CFADs", size=14, fontweight="bold")

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_cfad_diffs.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, cbar = [None] * 6

fig = plt.figure(figsize=(5, 4))
gs = fig.add_gridspec(1, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = plt.contourf(
    cfad_iop1_iop2,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    cmap="RdBu_r",
    zorder=0,
)
ax1.set_xticks(range(0, 16, 2), range(-10, 70, 10))
ax1.set_yticks(ax1.get_yticks(), range(2, 17, 2))
ax1.set_xlabel("Reflectivity (dBZ)")
ax1.set_ylabel("Height (km)")
ax1.set_title("Clusters CFADs IOP1 - IOP2", fontweight="bold")
cbar = plt.colorbar()
cbar.ax.set_ylabel("Frequency (%)")
plt.contour(
    cfad_iop1_iop2,
    extend="both",
    levels=np.arange(-20, 22, 2.5),
    colors="k",
    linewidths=0.5,
)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_cfad_diff_iops.png", dpi=300, facecolor="none"
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, cbar = [None] * 4


# N. NAE
print("---- Plotting NAE ----")
fig = plt.figure(figsize=(7, 10))
gs = fig.add_gridspec(5, 1)

ax1 = fig.add_subplot(gs[0, 0])
axplot = systems_all.nae.plot.hist(
    bins=range(-47000, 1500, 500), grid=True, color="k", edgecolor="k", ax=ax1
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_title("All Clusters")
axplot.set_title("a", loc="left", fontweight="bold", size=16)

ax2 = fig.add_subplot(gs[1, 0])
axplot = systems_all.loc[systems_all.geom_name.isin(names_2h)].nae.plot.hist(
    bins=range(-47000, 1500, 500), grid=True, color="k", edgecolor="k", ax=ax2
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_title(
    "Convective Systems ≤ 2h (total = "
    + str(systems_all.loc[systems_all.geom_name.isin(names_2h)].count().nae)
    + ")"
)
axplot.set_title("b", loc="left", fontweight="bold", size=16)

ax3 = fig.add_subplot(gs[2, 0])
axplot = systems_all.loc[systems_all.geom_name.isin(names_4h)].nae.plot.hist(
    bins=range(-47000, 1500, 500), grid=True, color="k", edgecolor="k", ax=ax3
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_title(
    "Convective Systems > 2h - ≤ 4h (total = "
    + str(systems_all.loc[systems_all.geom_name.isin(names_4h)].count().nae)
    + ")"
)
axplot.set_title("c", loc="left", fontweight="bold", size=16)

ax4 = fig.add_subplot(gs[3, 0])
axplot = systems_all.loc[systems_all.geom_name.isin(names_6h)].nae.plot.hist(
    bins=range(-47000, 1500, 500), grid=True, color="k", edgecolor="k", ax=ax4
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_title(
    "Convective Systems > 4h - ≤ 6h (total = "
    + str(systems_all.loc[systems_all.geom_name.isin(names_6h)].count().nae)
    + ")"
)
axplot.set_title("d", loc="left", fontweight="bold", size=16)

ax5 = fig.add_subplot(gs[4, 0])
axplot = systems_all.loc[systems_all.geom_name.isin(names_maxh)].nae.plot.hist(
    bins=range(-47000, 1500, 500), grid=True, color="k", edgecolor="k", ax=ax5
)
axplot.set_yscale("log")
axplot.set_ylim(bottom=1)
axplot.set_ylabel("Count")
axplot.set_xlabel("$10^6 s^{-1}$")
axplot.set_title(
    "Convective Systems > 6h (total = "
    + str(systems_all.loc[systems_all.geom_name.isin(names_maxh)].count().nae)
    + ")"
)
axplot.set_title("e", loc="left", fontweight="bold", size=16)

fig.suptitle(
    "Normalized Area Expansion of Clusters", size=14, fontweight="bold"
)

gs.tight_layout(fig)

plt.savefig(
    figpath + "exploratory_stats_nae.png",
    dpi=300,
    facecolor="none",
)

plt.cla()
plt.clf()
plt.close(fig)
fig, gs, ax1, ax2, ax3, ax4, ax5, axplot = [None] * 8
"""
