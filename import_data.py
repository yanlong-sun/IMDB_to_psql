"""
Restore the IMDb data to POSTGRESQL
"""
import psycopg2
import os, gzip
from io import StringIO
from dask import dataframe as dd
import argparse

class ImportData:
    def __init__(self, dbname, username, password, folder_path):
        self.folder_path = folder_path
        self.dbname = dbname
        self.username = username
        self.password = password
        self.connect_cmd = "dbname="+ self.dbname + " " + "user=" + self.username + " " + "password=" + self.password
        self.dtype_dict = {
           "titleakas": {
                'titleId': 'text',
                'ordering': 'integer',
                'title': 'text',
                'region': 'text',
                'language': 'text',
                'types': 'text[]',
                'attributes': 'text[]',
                'isOriginalTitle': 'boolean'
            },
            "titlebasics": {
                'tconst': 'text',
                'titleType': 'text',
                'primaryTitle': 'text',
                'originalTitle': 'text',
                'isAdult': 'boolean',
                'startYear': 'integer',
                'endYear': 'integer',
                'runtimeMinutes': 'integer',
                'genres': 'text[]'
            },
            "titlecrew":{
                'tconst': 'text',
                'directors': 'text[]',
                'writers': 'text[]'
            },
            "titleepisode": {
                'tconst': 'text',
                'parentTconst': 'text',
                'seasonNumber': 'integer',
                'episodeNumber': 'integer'
            },
            "titleprincipals": {
                'tconst': 'text',
                'ordering': 'integer',
                'nconst': 'text',
                'category': 'text',
                'job': 'text',
                'characters': 'text'
            },
            "titleratings": {
                'tconst': 'text',
                'averageRating': 'numeric(3,1)',
                'numVotes': 'integer'
            },
            "namebasics": {
                'nconst': 'text',
                'primaryName': 'text',
                'birthYear': 'integer',
                'deathYear': 'integer',
                'primaryProfession': 'text[]',
                'knownForTitles': 'text[]'
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
        self.number = 1

    def import_all(self):
        for datafile in [x for x in os.listdir(self.folder_path) if x.endswith('tsv.gz')]:
            dataname = ('').join(datafile.split('.')[:2])
            print(f"#{self.number} start restoring {dataname}")
            # read data
            data_path = os.path.join(self.folder_path, datafile)
            with gzip.open(data_path, 'rb') as f:
                df = dd.read_csv(data_path, sep = '\t', dtype=object, blocksize=None)
            df = df.replace('\\N', "")
            df = df.set_index(self.pks[dataname])
            # create empty table in DB
            conn = psycopg2.connect(self.connect_cmd)
            cursor = conn.cursor()
            schema = 'public'
            primary_key = self.pks[dataname]
        
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
            query_string = f'CREATE TABLE IF NOT EXISTS {schema}.{dataname} ({", ".join([f"{col} {self.dtype_dict[dataname][col]}" for col in df.columns])})'
            print(query_string)
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {schema}.{dataname} ({", ".join([f"{col} {self.dtype_dict[dataname][col]}" for col in df.columns])})')
            cursor.execute(f'ALTER TABLE {schema}.{dataname} ADD PRIMARY KEY ({primary_key})')
            
            # load data to DB
            # Write data to table
            err_tables = []
            for n in range(df.npartitions):
                data = df.get_partition(n).compute()
                output = StringIO()
                data.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                try:
                    cursor.copy_from(output, f'{schema}.{dataname}', null='')
                except Exception:
                    print(Exception)
                    err_tables.append(data)
                    conn.rollback()
                    continue
                conn.commit()

            # Close database connection
            cursor.close()
            conn.close()
            print(f"{dataname} finished.")
            self.number += 1

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
    print("\n")
    i = ImportData(dbname=args.dbname, 
                   username=args.username, 
                   password=args.password, 
                   folder_path=folder_path)
    i.import_all()
