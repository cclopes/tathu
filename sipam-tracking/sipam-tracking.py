# Adapted from tathu_old/examples/radar-case.py for Py-ART support
# Data used: SIPAM S-Band CAPPIS available at:
# https://adc.arm.gov/discovery/#/results/id::3900_macro_sipam-s-band-cappi_cloud_radardoppler?showDetails=true

import configparser
import sys
import numpy as np

from tathu.io import icsv, spatialite
from tathu.tracking import descriptors
from tathu.tracking import detectors
from tathu.tracking import trackers
from tathu.utils import array2raster, file2timestamp, extractPeriods, Timer
from tathu.constants import KM_PER_DEGREE

from read_sipam_cappis_cptec import read_simple_cappi


def get_files(filepath):
    """Return filelist according to a txt file"""
    filelist = open(filepath, "r")
    files = [line.strip() for line in filelist]
    return files


def read_data(path, level, is_small_coverage):
    """Reading radar data (specifically CPTEC SIPAM CAPPIs)"""

    print("Reading data")
    cappi, coords = read_simple_cappi(path, "/".join(path.split("/")[:-3]))
    if is_small_coverage:
        # Considering small coverage as 150 x 150 km from 240 x 240 km
        subset = slice(45, -45)
        # Define data extent
        extent = [
            coords[0][subset, subset].min(),
            coords[1][subset, subset].min(),
            coords[0][subset, subset].max(),
            coords[1][subset, subset].max(),
        ]
        array = np.flipud(cappi[level][subset, subset])
    else:
        # Define data extent
        # extent = [0.0, 0.0, 200.0, 200.0] # Un-geolocated
        extent = [
            coords[0].min(),
            coords[1].min(),
            coords[0].max(),
            coords[1].max(),
        ]
        array = np.flipud(cappi[level])
    # return array2raster(array, extent, nodata=0)
    return array2raster(array, extent, nodata=-99.), extent


# def read_data(path, level, is_small_coverage):
#     """Reading radar data (specifically ARM SIPAM CAPPIs)"""

#     print("Reading data")
#     cappi = read_sipam_cappi(path)
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
#             np.ma.filled(cappi.fields["DBZc"]["data"][level][subset, subset], -999)
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
#         array = np.flipud(np.ma.filled(cappi.fields["DBZc"]["data"][level], -999))
#     # return array2raster(array, extent, nodata=0)
#     return array2raster(array, extent, nodata=-999), extent


def detect(
    path,
    is_small_coverage,
    level,
    date_regex,
    date_format,
    is_multi_threshold,
    threshold,
    threshold_2,
    min_area,
    min_area_2,
    stats,
):
    """Detecting systems in the radar image using settings provided"""

    with Timer():
        # Extract file timestamp
        timestamp = file2timestamp(path, date_regex, date_format)

        print("Searching for systems at: ", timestamp)

        # Get data
        grid, extent = read_data(path, level, is_small_coverage)

        # Define minimum area of systems (km^2)
        if not is_multi_threshold:
            # Single threshold
            # Convert to degrees^2
            minarea = min_area / (KM_PER_DEGREE * KM_PER_DEGREE)
        else:
            # Multi-threshold
            minarea = [min_area, min_area_2]
            # Convert to degrees^2
            minarea = [i / (KM_PER_DEGREE * KM_PER_DEGREE) for i in minarea]

        # Create detector
        if not is_multi_threshold:
            # Single threshold
            detector = detectors.ThresholdDetector(
                threshold,
                detectors.ThresholdOp.GREATER_THAN,
                minarea,
            )
        else:
            # Multi-threshold
            detector = detectors.MultiThresholdDetector(
                [threshold, threshold_2],
                detectors.ThresholdOp.GREATER_THAN,
                minarea,
            )

        # Searching for systems
        systems = detector.detect(grid)

        # Adjust timestamp and add the number of layers
        # nlayers = 1 has 40 dBZ layer
        for s in systems:
            s.timestamp = timestamp
            nlayers = {"nlayers": len(s.layers)}
            # print(nlayers)
            s.attrs.update(nlayers)

        # print(systems[0].attrs.keys())

        # Create statistical descriptor
        if not is_multi_threshold:
            # Single threshold
            descriptor = descriptors.DBZStatisticalDescriptor(stats=stats, rasterOut=True)
        else:
            # Multi-threshold, remove nlayers
            descriptor = descriptors.DBZStatisticalDescriptor(stats=stats[:-1], rasterOut=True)

        # Describe systems (stats)
        descriptor.describe(grid, systems)

        grid = None

        return systems


def track(
    files,
    is_small_coverage,
    level,
    date_regex,
    date_format,
    stats,
    is_multi_threshold,
    threshold,
    threshold_2,
    min_area,
    min_area_2,
    area_overlap,
    outputter,
):
    """Tracking subsequent images based on detection"""

    # try:
    # Detect first systems
    current = detect(
        files[0],
        is_small_coverage,
        level,
        date_regex,
        date_format,
        stats,
        is_multi_threshold,
        threshold,
        threshold_2,
        min_area,
        min_area_2,
    )

    # Save to output
    outputter.output(current)

    # Prepare tracking...
    previous = current

    # Create overlap area strategy
    strategy = trackers.RelativeOverlapAreaStrategy(area_overlap)

    # for each image file
    for i in range(1, len(files)):
        # Detect current systems
        current = detect(
            files[i],
            is_small_coverage,
            level,
            date_regex,
            date_format,
            stats,
            is_multi_threshold,
            threshold,
            threshold_2,
            min_area,
            min_area_2,
        )

        # Let's track!
        t = trackers.OverlapAreaTracker(previous, strategy=strategy)
        t.track(current)

        # Save to output
        outputter.output(current)

        # Prepare next iteration
        previous = current

    print("Done!")
    # except Exception as e:
    #     print("Unexpected error:", e, sys.exc_info()[0])


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

# Print infos
print("== Tathu - Tracking and Analysis of Thunderstorms ==")
print(":: Start Date:", date_start)
print(":: End Date:", date_end)
print(":: Config tracking file location:", "sipam_tracking/config_sipam.ini")
print(":: Filepaths:", data_in)
print(":: Date Regex:", date_regex)
print(":: Date Format:", date_format)
print(":: Minimum accepted time interval between two images:", timeout, "minutes")
print(":: Is multi threshold:", is_multi_threshold)
print(":: Reflectivity threshold:", threshold, ", ", threshold_2, "dBZ")
print(":: Minimum area of systems:", minarea, ", ", minarea_2, "km2")
print(":: Area Overlap:", areaoverlap * 100, "%")
print(":: Stats:", stats)

# Extracting periods
periods = extractPeriods(files, timeout, date_regex, date_format)
print(periods)

# Export results
if type_database == "csv":
    db = icsv.Outputter(
        database + ".csv", writeHeader=True, outputGeom=True, outputCentroid=True
    )
elif type_database == "sqlite":
    db = spatialite.Outputter(database + ".sqlite", "systems", stats)

# Executing tracking
for period in periods:
    track(
        period,
        is_small_coverage,
        level,
        date_regex,
        date_format,
        is_multi_threshold,
        threshold,
        threshold_2,
        minarea,
        minarea_2,
        stats,
        areaoverlap,
        db,
    )
