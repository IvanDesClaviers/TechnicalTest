mypassword = "Shadowor+123"

import psycopg2

conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres",
                        password=mypassword, port=5432)

curr = conn.cursor()

curr.execute("""DROP TABLE IF EXISTS MilaTechnicalTestDB;""")

curr.execute("""    
CREATE TABLE MilaTechnicalTestDB (
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

""")


curr.execute("""COPY MilaTechnicalTestDB 
             FROM 'C:\\Users\\ivana\\Desktop\\PythonProjects\\enriched.csv' 
             CSV HEADER;
;
""")

conn.commit()

curr.close()
conn.close()
