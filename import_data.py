"""
Restore the IMDb data to POSTGRESQL
"""
import psycopg2
import pandas as pd
import csv, os, io, gzip
from io import StringIO
from sqlalchemy import create_engine
from dask import dataframe as dd
import argparse
import numpy as np

class ImportData:
    def __init__(self, dbname, username, password, folder_path):
        self.folder_path = folder_path
        self.dbname = dbname
        self.username = username
        self.password = password
        self.connect_cmd = "dbname="+ self.dbname + " " + "user=" + self.username + " " + "password=" + self.password
        self.create_engine_cmd = "postgresql://" + self.username + ":" + self.password + "@localhost:5432/" + self.dbname
        self.dtype_dict = {
            "titleakas": {
                'titleId': 'string',
                'ordering': 'int',
                'title': 'string',
                'region': 'string',
                'language': 'string',
                'types': 'object',
                'attributes': 'object',
                'isOriginalTitle': 'boolean'
            },
            "titlebasics": {
                'tconst': 'string',
                'titleType': 'string',
                'primaryTitle': 'string',
                'originalTitle': 'string',
                'isAdult': 'boolean',
                'startYear': 'Int64',
                'endYear': 'Int64',
                'runtimeMinutes': 'Int64',
                'genres': 'object'
            },
            "titlecrew":{
                'tconst': 'string',
                'directors': 'object',
                'writers': 'object'
            },
            "titleepisode": {
                'tconst': 'string',
                'parentTconst': 'string',
                'seasonNumber': 'Int64',
                'episodeNumber': 'Int64'
            },
            "titleprincipals": {
                'tconst': 'string',
                'ordering': 'int',
                'nconst': 'string',
                'category': 'string',
                'job': 'string',
                'characters': 'string'
            },
            "titleratings": {
                'tconst': 'string',
                'averageRating': 'float64',
                'numVotes': 'Int64'
            },
            "namebasics": {
                'nconst': 'string',
                'primaryName': 'string',
                'birthYear': 'Int64',
                'deathYear': 'Int64',
                'primaryProfession': 'object',
                'knownForTitles': 'object'
            }
        }
        self.pks = {
            "titleakas": 'titleId',
            "titlebasics": 'tconst',
            "titlecrew": 'tconst',
            "titleepisode": 'tconst',
            "titleprincipals":'tconst',
            "titleratings": 'tconst',
            "namebasics": 'nconst'
        }

    def import_all(self):
        for datafile in [x for x in os.listdir(self.folder_path) if x.endswith('tsv.gz')]:
            dataname = ('').join(datafile.split('.')[:2])
            # read data
            data_path = os.path.join(self.folder_path, datafile)
            with gzip.open(data_path, 'rb') as f:
                df = dd.read_csv(data_path, sep = '\t', dtype=object)
            df = df.replace('\\N', "")
            df = df.set_index(self.pks[dataname])
            # create empty table in DB
            conn = psycopg2.connect(self.connect_cmd)
            cursor = conn.cursor()
            schema = 'public'
            primary_key = self.pks[dataname]

            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {schema}.{dataname} ({", ".join([f"{col} {self.dtype_dict[dataname][col]}" for col in df.columns])}, PRIMARY KEY ({primary_key}))')

            
            # load data to DB
            # Write data to table
            for n in range(df.npartitions):
                data = df.get_partition(n).compute()
                output = StringIO()
                data.to_csv(output, sep='\t', header=False, index=True)
                output.seek(0)
                cursor.copy_from(output, f'{schema}.{dataname}', sep='\t', null='')
                conn.commit()

            # Close database connection
            cursor.close()
            conn.close()
            print(f"{dataname} finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import IMDb datasets into the database",
                                     epilog="Use -h for help")
    parser.add_argument('--dbname', type=str, required=True, help='The name of the database.')
    parser.add_argument('--username', type=str, required=True, help='The username to connect to the database.')
    parser.add_argument('--password', type=str, required=True, help='The password to connect to the database.')
    parser.add_argument('--folder_path', type=str, help='The path to the folder containing the data.')

    args = parser.parse_args()

    print(f"Database name: {args.dbname}")
    print(f"Username: {args.username}")
    print(f"Password: {args.password}")
    folder_path = args.folder_path if args.folder_path else '/Users/yanlongsun/Downloads/imdb_data/'  
    print(f"Dataset Path: {folder_path}")
    i = ImportData(dbname=args.dbname, 
                   username=args.username, 
                   password=args.password, 
                   folder_path=folder_path)
    i.import_all()
