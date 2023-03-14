"""
Restore the IMDb data to POSTGRESQL
"""
import psycopg2
import pandas as pd
import csv
import os
import io
import gzip
from io import StringIO
from sqlalchemy import create_engine
from dask import dataframe as dd
import argparse


class ImportData:
    def __init__(self, dbname, username, password, folder_path):
        self.folder_path = folder_path
        self.dbname = dbname
        self.username = username
        self.password = password
        self.connect_cmd = "dbname=" + self.dbname + " " + "user=" + \
            self.username + " " + "password=" + self.password
        self.create_engine_cmd = "postgresql://" + self.username + \
            ":" + self.password + "@localhost:5432/" + self.dbname

    def import_partial(self):
        for datafile in [x for x in os.listdir(self.folder_path)
                         if x.endswith('tsv.gz') and x not in
                         ['title.akas.tsv.gz', 'title.principals.tsv.gz']]:
            dataname = ''.join(datafile.split('.')[:2])
            with gzip.open(os.path.join(self.folder_path, datafile), 'rb') as f:
                # read data
                df = pd.read_csv(f, sep='\t', chunksize=10000)
                pd_df = pd.concat(df)
            pd_df = pd_df.replace('\\N', '')
            # load data to DB
            conn = psycopg2.connect(self.connect_cmd)
            engine = create_engine(self.create_engine_cmd)
            pd_df.to_sql(dataname, engine, method=self.psql_insert_copy)
            # control operation
            with conn.cursor() as curs:
                curs.execute("""
                select count(*) from """+dataname+"""
                            """)
                datacount = curs.fetchone()
            print(f"Finished {dataname}, {datacount[0]} rows restored")
            del pd_df

    def import_rest(self):
        for datafile in [x for x in os.listdir(self.folder_path)
                         if x.endswith('tsv.gz') and x in
                         ['title.akas.tsv.gz', 'title.principals.tsv.gz']]:
            dataname = ('').join(datafile.split('.')[:2])
            # read data
            data_path = os.path.join(self.folder_path, datafile)
            if datafile == 'title.akas.tsv.gz':
                with gzip.open(data_path, 'rb') as f:
                    dask_df = dd.read_csv(data_path, sep='\t', dtype={
                                          'isOriginalTitle': 'object'}, blocksize=None)
                dask_df = dask_df.replace('\\N', '')
            else:
                with gzip.open(data_path, 'rb') as f:
                    dask_df = dd.read_csv(data_path, sep='\t', blocksize=None)
                dask_df = dask_df.replace('\\N', '')

            # create empty table in DB
            conn = psycopg2.connect(self.connect_cmd)
            engine = create_engine(self.create_engine_cmd)

            pd.DataFrame(columns=dask_df.columns).to_sql(
                dataname,
                con=engine,
                if_exists='replace',
                index=False)
            err_tables = []
            # load data to DB
            with conn.cursor() as curs:
                for n in range(dask_df.npartitions):
                    table_chunk = dask_df.get_partition(n).compute()
                    output = io.StringIO()
                    table_chunk.to_csv(
                        output, sep='\t', header=False, index=False)
                    output.seek(0)
                    try:
                        curs.copy_from(output, dataname, null='')
                    except Exception:
                        err_tables.append(table_chunk)
                        conn.rollback()
                        continue
                    conn.commit()

                # check if data loaded to DB
                curs.execute("""
                select count(*) from """+dataname+"""
                            """)

                datacount = curs.fetchone()
            # delete dataframe to lighten memory
            print(f"Finished {dataname}, {datacount[0]} rows restored")
            del dask_df

    def psql_insert_copy(self, table, conn, keys, data_iter):
        """
        Execute SQL statement inserting data
        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    def preprocessing(self):
        conn = psycopg2.connect(self.connect_cmd)
        cur = conn.cursor()
        with open("./alter_table.sql", "r") as f:
            sql_code = f.read()
        cur.execute(sql_code)
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import IMDb datasets into the database",
                                     epilog="Use -h for help")
    parser.add_argument('--dbname', type=str, required=True,
                        help='The name of the database.')
    parser.add_argument('--username', type=str, required=True,
                        help='The username to connect to the database.')
    parser.add_argument('--password', type=str, required=True,
                        help='The password to connect to the database.')
    parser.add_argument('--folder_path', type=str,
                        help='The path to the folder containing the data.')

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
    i.import_partial()
    i.import_rest()
    i.preprocessing()
