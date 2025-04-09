import psycopg2

file_path = "C:\\Users\\ivana\\Desktop\\PythonProjects\\enriched.csv"
mypassword = "YOURPASSWORD"
db_name = "MilaTechnicalTestDB"

conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres",
                        password=mypassword, port=5432)

curr = conn.cursor()

curr.execute("""DROP TABLE IF EXISTS %s;""" % (db_name))

curr.execute("""    
CREATE TABLE %s (
    MovementDateTime DATE ,
    Destination TEXT,
    DestinationTidied TEXT,
    Speed FLOAT,
    AdditionalInfo TEXT,
    CallSign TEXT,
    Heading FLOAT,
    MMSI FLOAT,
    MovementID REAL,
    ShipName TEXT ,
    ShipType TEXT ,
    Beam FLOAT,
    Draught FLOAT,
    Length REAL,
    ETA DATE,
    MoveStatus TEXT,
    ladenStatus TEXT ,
    LRIMOShipNo REAL,
    Latitude FLOAT,
    Longitude FLOAT,
    BeamRatio FLOAT
);

""" % (db_name))


curr.execute("""COPY %s 
             FROM '%s' 
             CSV HEADER;
;""" % (db_name, file_path))

conn.commit()

curr.close()
conn.close()
