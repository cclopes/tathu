import numpy as np

from tathu.io import pgis

# Setup informations to load systems from database
host = "localhost"
database = "goamazon_geo"
user = "postgres"
pwd = "postgres"
table = "systems"

# Load database
db = pgis.Loader(host, database, user, pwd, table)
names = db.loadNames()
print(len(names))

# Filter systems
# by queries
# FAZER QUERY ÚNICA (USAR DBEAVER COMO BASE)
grid_extent = (
    "'POLYGON(("
    + "-61.300000 -4.450000, -58.700000 -4.450000, "
    + "-58.700000 -1.800000, -61.300000 -1.800000, "
    + "-61.300000 -4.450000))'"
)
fams_error = (
    "('c8a8ed48-2db2-4eb7-b5e4-0feaf6452c5e',"
    + "'1a332204-12fe-4abb-bd9d-b73f5450dd03',"
    + "'c5f70bb0-5cb3-4b09-83f9-fa003b938f65',"
    + "'50f14b96-efa7-4dee-aa1b-510ece86172a')"
)
query_to_rm = (
    "SELECT name FROM systems "
    + "WHERE ST_Overlaps(ST_Envelope(geom), ST_PolygonFromText("
    + grid_extent
    + ", 4326)) "
)
queried_rm = np.unique([q[0] for q in db.query(query_to_rm)])
print(len(queried_rm))
query_to_sel = (
    "SELECT DISTINCT name FROM systems "
    + "WHERE nlayers > 0 "
    + "AND name NOT IN "
    + "('" + "','".join(queried_rm) + "')"
)
queried = [q[0] for q in db.query(query_to_sel)]
print(len(queried))
# 
query_to_apply = (
    "CREATE TABLE systems_v2 AS"
    + "SELECT * FROM systems "
    + "WHERE name IN "
    + "(" + ",".join(queried) + ")"
)
print(queried[100])

# NÃO DELETAR, CRIAR TABELA NOVA COM RESULTADOS
"""
# Deleting systems

# 1. Select families with 40 dBZ layer and remove them from original list
query_40dbz = "SELECT DISTINCT name FROM systems WHERE nlayers > 0"
40dbz_fams = tuple([q[0] for q in db.query(query_40dbz)])
names_to_remove = [name for name in names if name not in 40dbz_fams]
cur = db.conn.cursor()
cur.execute("DELETE FROM systems WHERE name IN " + str(names))
db.conn.commit()
cur.close()
db.conn.close()

# FALTA ADICIONAR OS QUE NÃO TEM NLAYERS = 1
# E MELHORAR ESSE OVERLAPS, TALVEZ?
query_borders = (
    "SELECT DISTINCT name FROM systems "
    + "WHERE ST_Overlaps(ST_Envelope(geom), ST_PolygonFromText("
    + grid_extent
    + ", 4326))"
)
query_40dbz = "SELECT DISTINCT name FROM systems WHERE nlayers > 0"
names = tuple([q[0] for q in db.query(query_40dbz)])
print(len(names))
print(dir(db))
cur = db.conn.cursor()
cur.execute("DELETE FROM systems WHERE name IN " + str(names))
db.conn.commit()
cur.close()
db.conn.close()
"""