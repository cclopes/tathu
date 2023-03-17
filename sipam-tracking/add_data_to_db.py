#%%
import configparser
import numpy as np
from datetime import datetime
import pickle
import psycopg2

import pandas as pd
import geopandas as gpd
import psycopg2
from osgeo import gdal, gdal_array
from skimage.draw import polygon2mask
import matplotlib.pyplot as plt

from tathu.io import pgis
from tathu import visualizer
from tathu.utils import (
    extractPeriods,
    file2timestamp,
    getGeoT,
    geo2grid,
)
from tathu.geometry.transform import shapely2ogr
from tathu.geometry.utils import extractCoordinates
from tathu.constants import LAT_LON_WGS84
from tathu.io.pgis import bytea2nparray, _adapt_array

from read_sipam_cappis_cptec import read_simple_cappi


# Tell pgis how to deal with numpy arrays
psycopg2.extensions.register_adapter(np.ndarray, _adapt_array)


def read_gld(filename):
    gld_pd = pd.read_csv(filename)
    gld_gpd = gpd.GeoDataFrame(
        gld_pd, geometry=gpd.points_from_xy(gld_pd.lon, gld_pd.lat)
    )
    gld_gpd["geom_ogr"] = [shapely2ogr(gld) for gld in gld_gpd.geometry]

    return gld_gpd


def read_radar(path, family_ext=False):
    """Reading radar data (specifically CPTEC SIPAM CAPPIs)"""

    cappi, coords = read_simple_cappi(path, "/".join(path.split("/")[:-3]))

    # Get original extent
    extent = [
        np.nanmin(coords[0]),
        np.nanmin(coords[1]),
        np.nanmax(coords[0]),
        np.nanmax(coords[1]),
    ]
    # print("original extent: ", extent)
    # Get grid original resolution
    gt = getGeoT(extent, 500, 500)  # CHECK RESOLUTION
    # print(gt)
    if family_ext is not False:
        # print("family extent: ", family_ext)
        # Creating new extent
        upper_left_geo = (family_ext[0], family_ext[3])
        lower_right_geo = (family_ext[2], family_ext[1])
        upper_left_grid = geo2grid(upper_left_geo[0], upper_left_geo[1], gt)
        lower_right_grid = geo2grid(lower_right_geo[0], lower_right_geo[1], gt)
        # print("upper_left: ", upper_left_grid)
        # print("lower_right: ", lower_right_grid)
        min_lin = upper_left_grid[0]
        max_lin = lower_right_grid[0]
        min_col = upper_left_grid[1]
        max_col = lower_right_grid[1]
        slice_lon = slice(min_col, max_col)
        slice_lat = slice(min_lin, max_lin)
        # print("slice_lon: ", slice_lon)
        # print("slice_lat: ", slice_lat)
        extent = [
            np.nanmin(coords[0][slice_lat, slice_lon]),
            np.nanmin(coords[1][slice_lat, slice_lon]),
            np.nanmax(coords[0][slice_lat, slice_lon]),
            np.nanmax(coords[1][slice_lat, slice_lon]),
        ]
        # print("cappi new extent: ", extent)

        # Slice the data
        array = []
        for l in range(len(cappi)):
            array.append(np.flipud(cappi[l])[slice_lat, slice_lon])
    else:
        array = []
        for l in range(len(cappi)):
            larray = np.flipud(cappi[l])
            larray[larray == -99.0] = np.nan
            array.append(larray)
    array = array[1:]
    return array, extent


# File paths
gld_path = "/home/camilacl/git/amazon-storms-aerosols/data/lightning/GLD_mod/"
radar_path = "/data2/GOAMAZON/radar/sipam_manaus/cptec_cappi/"

# Load family
db = pgis.Loader(
    "localhost", "goamazon_geo", "postgres", "postgres", "systems_filtered"
)
# Get systems
names = db.loadNames()
# print(len(names))
# Get dates
dates = db.loadDates()
print(len(dates))

"""
# Generating gld column
# Load by dates
for d in dates:
    # GLD data
    gld = read_gld(
        gld_path
        + d.strftime("%Y")
        + "/"
        + d.strftime("%m")
        + "/"
        + "GLD360_mod_"
        + d.strftime("%Y%m%d%H%M")
        + ".csv"
    )
    # print(gld)
    # Systems
    names_date = db.loadSystemsByDate(d)
    print(len(names_date))
    for syst in names_date:
        # Check if points are inside the system
        gld_count = [syst.geom.Contains(point) for point in gld.geom_ogr].count(
            True
        )
        # Add to database
        query = (
            "UPDATE systems_filtered SET gld = "
            + str(gld_count)
            + " WHERE name = '"
            + str(syst.name)
            + "' AND date_time = timestamp '"
            + str(syst.timestamp)
            + "'"
        )
        print(query)
        cur = db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query)
        db.conn.commit()
        cur.close()

# Generating echotops columns
for d in dates:
    # Radar data
    radar, extent = read_radar(
        radar_path
        + d.strftime("%Y")
        + "/"
        + d.strftime("%m")
        + "/"
        + "cappi_"
        + d.strftime("%Y%m%d%H%M")
        + ".dat.gz"
    )
    # print(type(radar.GetRasterBand(1)))
    # print(radar)
    # print(extent)
    gt = getGeoT(extent, 500, 500)

    # Systems
    names_date = db.loadSystemsByDate(d)
    print(len(names_date))
    for syst in names_date:
        # Check if points are inside the system
        # print(d)
        # print(syst.name)
        # print(type(syst.geom))

        # Extracting coordinates from geom and converting to x/y positions
        lats, lons = extractCoordinates(syst.geom)
        coords = [geo2grid(lon, lat, gt) for lat, lon in zip(lats, lons)]
        # print(coords)
        # Creating mask matrix from geom x/y positions
        mask = polygon2mask(radar[0].shape, coords)
        # print(mask)
        # Applying mask to radar data
        masked_radar = [np.ma.masked_array(r, np.invert(mask)) for r in radar]
        # print(masked_radar)
        # Plotting to see if everything's ok
        # plt.imshow(masked_radar[1])
        # plt.colorbar()
        # plt.show()

        # Filling echotops columns (BASIC CALCULATION)
        echotop_0 = 0.0
        echotop_20 = 0.0
        echotop_40 = 0.0
        for l in range(len(masked_radar) - 1, 0, -1):
            # print(l)
            unmasked_radar = np.ma.compressed(masked_radar[l])
            # print(unmasked_radar[~np.isnan(unmasked_radar)])
            if echotop_0 == 0.0:
                if np.any(unmasked_radar[~np.isnan(unmasked_radar)] >= 0.0):
                    echotop_0 = l + 2
            if echotop_20 == 0.0:
                if np.any(unmasked_radar[~np.isnan(unmasked_radar)] >= 20.0):
                    echotop_20 = l + 2
            if echotop_40 == 0.0:
                if np.any(unmasked_radar[~np.isnan(unmasked_radar)] >= 40.0):
                    echotop_40 = l + 2
        print(echotop_0, echotop_20, echotop_40)

        # Add to database
        query = (
            "UPDATE systems_filtered SET echotop_0 = %s, "
            + "echotop_20 = %s, echotop_40 = %s "
            + "WHERE name = %s AND date_time = timestamp %s"
        )
        # print(query)

        cur = db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            query,
            (
                str(echotop_0),
                str(echotop_20),
                str(echotop_40),
                str(syst.name),
                str(syst.timestamp),
            ),
        )
        db.conn.commit()
        cur.close()

# Generating CFAD, VIL, VII columns
for d in dates:
    # Radar data
    radar, extent = read_radar(
        radar_path
        + d.strftime("%Y")
        + "/"
        + d.strftime("%m")
        + "/"
        + "cappi_"
        + d.strftime("%Y%m%d%H%M")
        + ".dat.gz"
    )
    # print(type(radar.GetRasterBand(1)))
    # print(radar)
    # print(extent)
    gt = getGeoT(extent, 500, 500)

    # Systems
    names_date = db.loadSystemsByDate(d)
    print(len(names_date))
    for syst in names_date:
        # Check if points are inside the system
        # print(d)
        # print(syst.name)
        # print(type(syst.geom))

        # Extracting coordinates from geom and converting to x/y positions
        lats, lons = extractCoordinates(syst.geom)
        coords = [geo2grid(lon, lat, gt) for lat, lon in zip(lats, lons)]
        # print(coords)
        # Creating mask matrix from geom x/y positions
        mask = polygon2mask(radar[0].shape, coords)
        # print(mask)
        # Applying mask to radar data
        masked_radar = [np.ma.masked_array(r, np.invert(mask)) for r in radar]
        # Resize the data for only valid rows/cols accoding to 3-km height
        rows_del = [i for i, x in enumerate(masked_radar[1].mask.all(axis=1)) if x]
        cols_del = [i for i, x in enumerate(masked_radar[1].mask.all(axis=0)) if x]
        # print(rows_del)
        # print(cols_del)
        for l in range(len(masked_radar)):
            subarray = np.delete(masked_radar[l], rows_del, axis=0)
            masked_radar[l] = np.delete(subarray, cols_del, axis=1)
            # print(masked_radar[l].shape)
            # print(masked_radar[l])
            # Plotting to see if everything's ok
            # plt.imshow(masked_radar[1])
            # plt.colorbar()
            # plt.show()
        
        # z_freq array
        z_freq = np.zeros((15, 10))

        # VIL/VII arrays
        vil = np.zeros(masked_radar[0].shape)
        vii = np.zeros(masked_radar[0].shape)

        # Filling z_freq array
        for l in range(len(masked_radar)):
            z_freq[l, 0] = (
                (masked_radar[l] >= 20) & (masked_radar[l] < 25)
            ).sum()
            z_freq[l, 1] = (
                (masked_radar[l] >= 25) & (masked_radar[l] < 30)
            ).sum()
            z_freq[l, 2] = (
                (masked_radar[l] >= 30) & (masked_radar[l] < 35)
            ).sum()
            z_freq[l, 3] = (
                (masked_radar[l] >= 35) & (masked_radar[l] < 40)
            ).sum()
            z_freq[l, 4] = (
                (masked_radar[l] >= 40) & (masked_radar[l] < 45)
            ).sum()
            z_freq[l, 5] = (
                (masked_radar[l] >= 45) & (masked_radar[l] < 50)
            ).sum()
            z_freq[l, 6] = (
                (masked_radar[l] >= 50) & (masked_radar[l] < 55)
            ).sum()
            z_freq[l, 7] = (
                (masked_radar[l] >= 55) & (masked_radar[l] < 60)
            ).sum()
            z_freq[l, 8] = (
                (masked_radar[l] >= 60) & (masked_radar[l] < 65)
            ).sum()
            z_freq[l, 9] = (
                (masked_radar[l] >= 65) & (masked_radar[l] < 70)
            ).sum()
        # print(z_freq)
        to_db_z_freq = z_freq.astype(np.int16)
        print("z_freq done!")

        # Filling VIL/VII arrays
        for l in range(len(masked_radar) - 1):
            meanarray = np.nanmean(
                np.array([masked_radar[l], masked_radar[l + 1]]), axis=0
            )
            meanarray = np.nan_to_num(meanarray)
            vil += (meanarray) ** (4 / 7)
            vii += (5.28e-18 / 720 * meanarray) ** (4 / 7)
        vil = 3.44e-6 * vil * 1000
        vii = np.pi * 917 * ((4e6) ** (3 / 7)) * vii * 1000
        # print(vil)
        # print(vii)
        # print(vil[np.where(vil != 0.0)])
        # print(vii[np.where(vii != 0.0)])
        vil = vil * 10000 #-- NEED TO CONVERT BACK WHEN OPENING!!!!
        vii = vii * 10000 #-- NEED TO CONVERT BACK WHEN OPENING!!!!
        to_db_vil = vil.astype(np.int16)
        to_db_vii = vii.astype(np.int16)
        print("VIL, VII done!")

        # print(to_db_z_freq)
        # print(to_db_vil)
        # print(to_db_vii)

        # Add to database
        query = (
            "UPDATE systems_filtered "
            + "SET z_freq = %s, vil_kgm2 = %s, vii_kgm2 = %s "
            + "WHERE name = %s AND date_time = timestamp %s"
        )
        print(query)

        cur = db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            query,
            (
                to_db_z_freq,
                to_db_vil,
                to_db_vii,
                str(syst.name),
                str(syst.timestamp),
            ),
        )
        db.conn.commit()
        cur.close()
"""

# Generating time derivations (area, lightning, echotops)
for name in names:
    fam = db.load(
        name, ["count", "gld", "echotop_0", "echotop_20", "echotop_40"]
    )

    # Getting data
    timestamps = fam.getTimestamps()
    areas = fam.getAttribute("count")
    glds = fam.getAttribute("gld")
    echotops_0 = fam.getAttribute("echotop_0")
    echotops_20 = fam.getAttribute("echotop_20")
    echotops_40 = fam.getAttribute("echotop_40")
    # print(timestamps)
    # print(areas)
    # print(glds)
    # print(echotops_0)
    # print(echotops_20)
    # print(echotops_40)

    # Generating delta cols
    nae = len(areas)*[None]
    delta_gld = len(glds)*[None]
    delta_echotops_0 = len(echotops_0)*[None]
    delta_echotops_20 = len(echotops_20)*[None]
    delta_echotops_40 = len(echotops_40)*[None]
    # print(nae)

    for i in range(1, len(timestamps)):
        delta = (timestamps[i] - timestamps[i - 1]).total_seconds()
        nae[i] = 1 / areas[i] * (areas[i] - areas[i - 1]) / delta  # [s-1]
        delta_gld[i] = (glds[i] - glds[i - 1]) / delta * 60  # [strokes/min]
        delta_echotops_0[i] = (
            (echotops_0[i] - echotops_0[i - 1]) / delta * 60
        )  # km/min
        delta_echotops_20[i] = (
            (echotops_20[i] - echotops_20[i - 1]) / delta * 60
        )  # km/min
        delta_echotops_40[i] = (
            (echotops_40[i] - echotops_40[i - 1]) / delta * 60
        )  # km/min
    print(nae)
    print(delta_gld)
    print(delta_echotops_0)
    print(delta_echotops_20)
    print(delta_echotops_40)

    # Add to database
    query = (
        "UPDATE systems_filtered "
        + "SET nae_s_1 = %s, gld_strmin = %s, "
        + "echotop0_kmmin = %s, echotop20_kmmin = %s, echotop40_kmmin = %s "
        + "WHERE name = %s AND date_time = timestamp %s"
    )
    print(query)
    
    cur = db.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for i in range(len(timestamps)):
        cur.execute(
            query,
            (
                nae[i],
                delta_gld[i],
                delta_echotops_0[i],
                delta_echotops_20[i],
                delta_echotops_40[i],
                str(name),
                str(timestamps[i]),
            ),
        )
        db.conn.commit()
    cur.close()
# %%
