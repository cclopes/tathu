# -*- coding: utf-8 -*-

# Plots adapted from tracking/visualizer.py

import os
import sys
import datetime
import gc
import configparser
import glob

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import cartopy
import pyart

from read_sipam_cappis import read_sipam_cappi
from read_sipam_cappis_cptec import read_simple_cappi

from tathu.io import spatialite, icsv
from tathu.visualizer import MapView
from tathu.constants import KM_PER_DEGREE
from tathu.utils import array2raster, file2timestamp

# Setup SpatiaLite extension
# os.environ["PATH"] = (
#     os.environ["PATH"] + ";../spatialite/mod_spatialite-4.3.0a-win-amd64"
# )


def get_files(filepath):
    """Return filelist according to a txt file"""
    filelist = open(filepath, "r")
    files = [line.strip() for line in filelist]
    return files


def get_radar_grid_extent(path, level, is_small_coverage):
    """Reading radar data (specifically CPTEC SIPAM CAPPIs)"""

    print("Reading data")
    cappi, coords = read_simple_cappi(path, "/".join(path.split("/")[:-3]))
    if is_small_coverage:
        # Considering small coverage as 150 x 150 km from 250 x 250 km
        subset = slice(100, -100)  # slice(50, -50)
        # Define data extent
        extent = [
            np.nanmin(coords[0][subset, subset]),
            np.nanmin(coords[1][subset, subset]),
            np.nanmax(coords[0][subset, subset]),
            np.nanmax(coords[1][subset, subset]),
        ]
        array = np.flipud(cappi[level][subset, subset])
    else:
        # Define data extent
        # extent = [0.0, 0.0, 200.0, 200.0] # Un-geolocated
        extent = [
            np.nanmin(coords[0]),
            np.nanmin(coords[1]),
            np.nanmax(coords[0]),
            np.nanmax(coords[1]),
        ]
        array = np.flipud(cappi[level])

    return array2raster(array, extent, nodata=-99.0), extent


# def get_radar_grid_extent(file, level, is_small_coverage):
#     """ Reading radar data (specifically ARM SIPAM CAPPIs)"""

#     print("Reading data")
#     cappi = read_sipam_cappi(file)
#     if is_small_coverage:
#         # Considering small coverage as 150 x 150 km from 240 x 240 km
#         subset = slice(45, -45)
#         # Define data extent
#         extent = [
#             cappi.get_point_longitude_latitude()[0][subset, subset].min(),
#             cappi.get_point_longitude_latitude()[1][subset, subset].min(),
#             cappi.get_point_longitude_latitude()[0][subset, subset].max(),
#             cappi.get_point_longitude_latitude()[1][subset, subset].max(),
#         ]
#         array = np.flipud(
#             np.ma.filled(
#                 cappi.fields["DBZc"]["data"][level][subset, subset], -999
#             )
#         )
#     else:
#         # Define data extent
#         # extent = [0.0, 0.0, 200.0, 200.0] # Un-geolocated
#         extent = [
#             cappi.get_point_longitude_latitude()[0].min(),
#             cappi.get_point_longitude_latitude()[1].min(),
#             cappi.get_point_longitude_latitude()[0].max(),
#             cappi.get_point_longitude_latitude()[1].max(),
#         ]
#         array = np.flipud(
#             np.ma.filled(cappi.fields["DBZc"]["data"][level], -999)
#         )
#     # return array2raster(array, extent, nodata=0)
#     return array2raster(array, extent, nodata=-999), extent


def extract_date(path):
    """Extracting date from filename"""

    dir, file = os.path.split(path)
    return file[6:14]  # yyyymmdd


def extract_hour(path):
    """Extracting hour from filename"""

    dir, file = os.path.split(path)
    return file[14:18]  # hhmm


def extract_timestamp(path):
    """Extracting date + hour and converting to timestamp"""

    day = extract_date(path)
    hour = extract_hour(path)
    return datetime.datetime.strptime(day + hour, "%Y%m%d%H%M")


def extract_periods(files, max_minutes):
    """Extracting periods between images and checking if is valid"""

    previous_time = None
    result = []
    period = []

    for path in files:
        # Get current date/time
        current_time = extract_timestamp(path)
        # Initialize, if necessary
        if previous_time is None:
            previous_time = current_time

        # Compute elapsed time
        elapsed_time = current_time - previous_time

        if elapsed_time.seconds > max_minutes * 60:
            result.append(period)
            period = []
            previous_time = None
        else:
            period.append(path)
            previous_time = current_time

    result.append(period)

    return result


def area2degrees(km2):
    return km2 / (KM_PER_DEGREE * KM_PER_DEGREE)


def get_families(db, timestamp, filter_names):
    # by date
    families = db.loadByDate("%Y-%m-%d %H:%M:%S", str(timestamp), [])
    families = [family for family in families if str(family.name) in filter_names]
    # print(families)
    return families


def plot_save_fig(systems, grid, extent, timestamp, shpfolder, figfolder):
    print("Plotting: " + str(timestamp))
    # Extract polygons
    p = []
    for s in systems:
        p.append(s.geom)

    # Visualize result
    m = MapView(
        extent,
        references=[
            shpfolder + "shapefiles/AM_Municipios_2019.shp",
            shpfolder + "shapefiles/BR_UF_2021.shp",
        ],
        clabel = "Reflectivity [dBZ]",
        timestamp = str(timestamp)
    )
    m.plotImage(grid, colorbar=True, cmap="pyart_HomeyerRainbow", vmin=0, vmax=70)
    m.plotPolygons(p, facecolor="none", lw=0.5, centroids=True)
    # m.show()
    plt.savefig(
        figfolder + "/figs/Families " + str(timestamp) + ".png",
        dpi=300,
    )

    del m, p

    gc.collect()


# Read config file and extract infos
params = configparser.ConfigParser(interpolation=None)
params.read("/home/camilacl/git/tathu/sipam-tracking/config_sipam.ini")

# Get input data parameters
data_in = params.get("input", "data_in")
date_regex = params.get("input", "date_regex")
date_format = params.get("input", "date_format")
level = int(params.get("input", "level_index"))
is_small_coverage = params.getboolean("input", "small_coverage")

# Get tracking parameters
timeout = float(params.get("tracking_parameters", "timeout"))
is_multi_threshold = params.getboolean("tracking_parameters", "multi_threshold")
threshold = float(params.get("tracking_parameters", "threshold"))
threshold_2 = float(params.get("tracking_parameters", "threshold_2"))
minarea = float(params.get("tracking_parameters", "minarea"))
minarea_2 = float(params.get("tracking_parameters", "minarea_2"))
areaoverlap = float(params.get("tracking_parameters", "areaoverlap"))
stats = [i.strip() for i in params.get("tracking_parameters", "stats").split(",")]

# Output
database = params.get("output", "data_out")
type_database = params.get("output", "type_data_out")

# Get files
files = get_files(data_in)

# Get date range
date_start = date_end = file2timestamp(files[0], date_regex, date_format)
if len(files) > 1:
    date_end = file2timestamp(files[-1], date_regex, date_format)

# Get images
print("Searching files...")
# if data_in is a path
# files = sorted(glob.glob(data_in))
# if data_in is a file
filelist = open(data_in, "r")
files = [line.strip() for line in filelist]
# print(files)

# Extracting timestamps
# print("Extracting timestamps...")
# timestamps = [extract_timestamp(file) for file in files]
# print(timestamps[0])

# Setup informations to load systems from database
dbname = database + ".sqlite"
table = "systems"

# Load table
print("Loading systems...")
db = spatialite.Loader(dbname, table)

# Read dataset for filtered systems
df = pd.read_csv(database + "_filter.csv", parse_dates=["timestamp"])

for file in files[:]:
    print(file)
    # Extract timestamp
    timestamp = extract_timestamp(file)
    # Get filtered names
    names = df[df["timestamp"] == timestamp]["name"].tolist()
    # print(names)
    # Get families
    families = get_families(db, timestamp, names)
    # Get grid
    grid, extent = get_radar_grid_extent(file, level, is_small_coverage)
    # print(grid.GetRasterBand(1))
    # Saving figure
    plot_save_fig(
        families,
        grid,
        extent,
        timestamp,
        "/home/camilacl/git/amazon-storms-aerosols/data/general/",
        "/home/camilacl/git/tathu/sipam-tracking/out",
    )
    # Delete files to save space
    del families, grid
