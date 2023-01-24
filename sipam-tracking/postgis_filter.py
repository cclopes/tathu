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
    + "AND ST_Overlaps(ST_Envelope(geom), ST_PolygonFromText("
    + grid_extent
    + ", 4326))"
)
queried = [q[0] for q in db.query(query)]
queried_filter = [q[0] for q in db.query(query_to_filter)]
print(len(queried))
names = [name for name in queried if name not in queried_filter]
# print(names)

# Deleting systems
# FALTA ADICIONAR OS QUE NÃO TEM NLAYERS = 1
# E MELHORAR ESSE OVERLAPS, TALVEZ?
query_to_filter = (
    "SELECT DISTINCT name FROM systems "
    + "WHERE ST_Overlaps(ST_Envelope(geom), ST_PolygonFromText("
    + grid_extent
    + ", 4326))"
)
names = tuple([q[0] for q in db.query(query_to_filter)])
# print(dir(db))
# cur = db.conn.cursor()
# cur.execute("DELETE FROM systems WHERE name IN " + str(names))
# db.conn.commit()
# cur.close()
# db.conn.close()
