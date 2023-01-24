# -*- coding: utf-8 -*-

# Adapted from examples/query-example.py

import os
import sys

from tathu.io import spatialite, icsv

# Setup informations to load systems from database
dbname = "/home/camilacl/git/tathu/sipam-tracking/out/test_dry_season.sqlite"
table = "systems"

# Load family
db = spatialite.Loader(dbname, table)
# print(db)

# Filter systems
# by queries
grid_extent = (
    "'POLYGON(("
    + "-61.343496 -4.505793, -58.640505 -4.505793, "
    + "-58.640505 -1.792021, -61.343496 -1.792021, "
    + "-61.343496 -4.505793))'"
)
query = "SELECT DISTINCT name FROM systems WHERE nlayers > 0"
query_to_filter = (
    "SELECT DISTINCT name FROM systems "
    + "WHERE nlayers > 0 "
    + "AND Overlaps(Envelope(geom), PolygonFromText("
    + grid_extent
    + ", 4326)) = 1"
)
queried = [q[0] for q in db.query(query)]
queried_filter = [q[0] for q in db.query(query_to_filter)]
# print(len(queried))
names = [name for name in queried if name not in queried_filter]
# print(len(names))
# to csv
outputter = icsv.Outputter(
    "/home/camilacl/git/tathu/sipam-tracking/out/test_dry_season_filter.csv",
    outputGeom=True,
    outputCentroid=True,
)
for name in names:
    family = db.load(name, ["max", "mean", "std", "count", "nlayers"])
    # print(family.systems[0].attrs)
    outputter.output(family.systems)


"""
# Test with duration queries
queries = []
queries.append(
    (
        "0-1h",
        "SELECT name FROM (SELECT name, (MAX(julianday(date_time)) - MIN(julianday(date_time))) * 24 AS elapsed_time FROM systems GROUP BY(name)) duration WHERE elapsed_time > 0 AND elapsed_time <= 1",
    )
)
queries.append(
    (
        "1-3h",
        "SELECT name FROM (SELECT name, (MAX(julianday(date_time)) - MIN(julianday(date_time))) * 24 AS elapsed_time FROM systems GROUP BY(name)) duration WHERE elapsed_time > 1 AND elapsed_time <= 3",
    )
)
queries.append(
    (
        "+3h",
        "SELECT name FROM (SELECT name, (MAX(julianday(date_time)) - MIN(julianday(date_time))) * 24 AS elapsed_time FROM systems GROUP BY(name)) duration WHERE elapsed_time > 3",
    )
)
# queries.append(
#     (
#         "6-9h",
#         "SELECT name FROM (SELECT name, (MAX(julianday(date_time)) - MIN(julianday(date_time))) * 24 AS elapsed_time FROM systems GROUP BY(name)) duration WHERE elapsed_time > 6 AND elapsed_time <= 9",
#     )
# )
# queries.append(
#     (
#         "9-12h",
#         "SELECT name FROM (SELECT name, (MAX(julianday(date_time)) - MIN(julianday(date_time))) * 24 AS elapsed_time FROM systems GROUP BY(name)) duration WHERE elapsed_time > 9 AND elapsed_time <= 12",
#     )
# )
# queries.append(
#     (
#         "12h",
#         "SELECT name FROM (SELECT name, (MAX(julianday(date_time)) - MIN(julianday(date_time))) * 24 AS elapsed_time FROM systems GROUP BY(name)) duration WHERE elapsed_time > 12",
#     )
# )

for q in queries:
    # Create result output
    outputter = icsv.Outputter(
        "/home/cclopes/git/tathu/sipam_tracking/out/test_wet_season_filter_"
        + q[0]
        + ".csv",
        writeHeader=True,
        delimiter=",",
        outputGeom=False,
        outputCentroid=True,
    )

    # Load names
    names = db.query(q[1])
    names = [name for name in names if name not in queried_filter]
    # print(names[0][0])

    # Exporting...
    print("Processing period", q[0], "...")
    for name in names:
        family = db.load(name[0], ["max", "mean", "std", "count", "nlayers"])
        outputter.output(family.systems)
    print("done!")
"""
