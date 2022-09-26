# Based on "examples/radar/animation-database.py"

import configparser
from datetime import datetime
from os import times
import numpy as np

from tathu.io import spatialite
from tathu import visualizer
from tathu.utils import extractPeriods, file2timestamp, array2raster, getGeoT, geo2grid

from read_sipam_cappis import read_sipam_cappi


def get_files(filepath):
    """Return filelist according to a txt file"""
    filelist = open(filepath, "r")
    files = [line.strip() for line in filelist]
    return files


def read_radar(path, level, family_ext = False):
    """Reading radar data (specifically SIPAM CAPPIs)"""

    cappi = read_sipam_cappi(path)

    if family_ext is not False:
        # Get original extent
        extent = [
            cappi.get_point_longitude_latitude()[0].min(),
            cappi.get_point_longitude_latitude()[1].max(),
            cappi.get_point_longitude_latitude()[0].max(),
            cappi.get_point_longitude_latitude()[1].min(),
        ]
        # print("original extent: ", extent)
        # Get grid original resolution
        gt = getGeoT(extent, 241, 241)
        # print(gt)
        # print("family extent: ", family_ext)
        # Creating new extent
        upper_left_geo = (family_ext[0], family_ext[1])
        lower_right_geo = (family_ext[2], family_ext[3])
        upper_left_grid = geo2grid(upper_left_geo[0], upper_left_geo[1], gt)
        lower_right_grid = geo2grid(lower_right_geo[0], lower_right_geo[1], gt)
        min_lin = upper_left_grid[0]
        max_lin = lower_right_grid[0]
        min_col = upper_left_grid[1]
        max_col = lower_right_grid[1]
        slice_lon = slice(min_col, max_col)
        slice_lat = slice(min_lin, max_lin)
        # print("slice_lon: ", slice_lon)
        # print("slice_lat: ", slice_lat)
        extent = [
            cappi.get_point_longitude_latitude()[0][slice_lat, slice_lon].min(),
            cappi.get_point_longitude_latitude()[1][slice_lat, slice_lon].min(),
            cappi.get_point_longitude_latitude()[0][slice_lat, slice_lon].max(),
            cappi.get_point_longitude_latitude()[1][slice_lat, slice_lon].max(),
        ]
        # print("cappi new extent: ", extent)

        # Slice the data
        array = np.flipud(cappi.fields["DBZc"]["data"][level][slice_lat, slice_lon])
    else:
        # Get original extent
        extent = [
            cappi.get_point_longitude_latitude()[0].min(),
            cappi.get_point_longitude_latitude()[1].min(),
            cappi.get_point_longitude_latitude()[0].max(),
            cappi.get_point_longitude_latitude()[1].max(),
        ]
        array = np.flipud(cappi.fields["DBZc"]["data"][level])

    return array, extent


# Setup informations to load systems from database
dbname = '/home/camila/git/tathu/sipam-tracking/out/carol_20140509.sqlite'
table = 'systems'

# Load family
db = spatialite.Loader(dbname, table)

# Read config file and extract infos
params = configparser.ConfigParser(interpolation=None)
params.read("/home/camila/git/tathu/sipam-tracking/config_sipam.ini")

# Get input data parameters
data_in = params.get("input", "data_in")
date_regex = params.get("input", "date_regex")
date_format = params.get("input", "date_format")
level = int(params.get("input", "level_index"))
# Get tracking parameters
timeout = float(params.get("tracking_parameters", "timeout"))

# Get files
files = get_files(data_in)
# print(files)

# Extracting periods
periods = extractPeriods(files, timeout, date_regex, date_format)
# print(periods)

# Extracting timestamps
timestamps = []
for period in periods:
    for file in period:
        timestamps.append(file2timestamp(file, date_regex, date_format))
# print(timestamps)

# Get images
images = []
for file in files:
    array, extent = read_radar(file, level, [-61.343496, -4.505793, -58.640505, -1.792021])
    # array = np.ma.masked_where(array == -99., array)
    images.append(array)

# Animation
view = visualizer.AnimationMapDatabase(db, extent, images, timestamps, cmap='pyart_HomeyerRainbow')

# Save animation to file?
saveAnimation = True

if saveAnimation:
    # Set up formatting for the movie files
    import matplotlib.animation as animation
    Writer = animation.writers['ffmpeg']
    writer = Writer(bitrate=-1)
    view.save('/home/camila/git/tathu/sipam-tracking/out/carol-20140509-animation.mp4')

view.show()