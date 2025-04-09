# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 16:32:22 2025

@author: ivana
"""

import boto3
import pandas as pd
import io
import datetime
import awswrangler as wr

# 6. Write a Lambda handler that gets triggered when a file that matches the filename (i.e 
# pace-data.csv) lands in the bucket you created above. 

# 7. This lambda function is responsible for executing the operations described in step 2. ( 
# wrap the code you wrote in step 2 to be executed within the lambda)

# RDS settings
import sys
import logging
# import psycopg2
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'automatically-created-bucket-ivanalt-mila'
    file_key = 'pace-data.txt'
    outfile_key = "enriched.csv"
    s3_address = "s3://%s/%s" % (bucket_name, outfile_key)
    database_name = "mila-bonus-point-db";

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')

        df = pd.read_csv(io.StringIO(csv_content))

        # Apply 2b
        df['MovementDateTime'] = df['MovementDateTime'].map(lambda x: datetime.datetime.fromisoformat(x).isoformat())
        
        # Apply 2c
        pd.options.mode.chained_assignment = None 
        CallSign_mean_value = df.groupby('CallSign').mean(numeric_only=True)
        CallSign_mean_speed_value = CallSign_mean_value["Speed"]
        df_weird_vals = df[(df["MoveStatus"] == "Under way using engine") &
                            (df["Speed"] == 0 | df["Speed"].isnull()) ]
        for index, row in df_weird_vals.iterrows():
            df_weird_vals["Speed"] = CallSign_mean_speed_value[row["CallSign"]]
        df.update(df_weird_vals)

        # Apply 2d
        df["BeamRatio"] = df["Beam"]/ df["Length"]

        # Apply 2e
        # Upload to s3
        wr.s3.to_csv(df, path=s3_address, index=False)

        # TODO: I tried the Rds connection and I was very close, but 
        # I struggled with the VPC to connect both, so here is the work done

        # Bonus RDS
        # user_name = os.environ['USER_NAME'] 
        # password = os.environ['PASSWORD']
        # rds_proxy_host = os.environ['HOST']
        # db_name = os.environ['DB_NAME']
        # logger = logging.getLogger()
        # logger.setLevel(logging.INFO)

        # # create the database connection outside of the handler to allow connections to be
        # # re-used by subsequent function invocations.
        # conn = psycopg2.connect(host=rds_proxy_host, user=user_name, passwd=password, db=db_name)

        # cur = conn.cursor()

        # curr.execute("""    
        # CREATE TABLE IF NOT EXIST %s (
        #     MovementDateTime DATE ,
        #     Destination TEXT,
        #     DestinationTidied TEXT,
        #     Speed FLOAT,
        #     AdditionalInfo TEXT,
        #     CallSign TEXT,
        #     Heading FLOAT,
        #     MMSI FLOAT,
        #     MovementID REAL,
        #     ShipName TEXT ,
        #     ShipType TEXT ,
        #     Beam FLOAT,
        #     Draught FLOAT,
        #     Length REAL,
        #     ETA DATE,
        #     MoveStatus TEXT,
        #     ladenStatus TEXT ,
        #     LRIMOShipNo REAL,
        #     Latitude FLOAT,
        #     Longitude FLOAT,
        #     BeamRatio FLOAT);""" % (database_name) )

        # # conn.commit() ?
        # curr.execute("""
        # LOAD DATA FROM S3 '%s'
        # INTO TABLE %s
        # FIELDS TERMINATED BY ','
        # LINES TERMINATED BY '\n'
        # IGNORE 1 ROWS;""" % (s3_address, database_name) )

        # conn.commit()
        # curr.close()


        return {
            'statusCode': 200,
            'body': ('%s parsed and augmented successfully into %s' % (file_key, outfile_key))
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
