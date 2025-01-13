"""Connect to databases"""
import assets
import pyodbc
import sqlalchemy as sa
import pandas as pd

def _db_connect(db:str) -> None:

    if db.lower() == 'db_2023_py':
        con_str = (
        r'driver={SQL Server};'
        r'server=(local);'
        f'database={assets.DB_2023};'
        r'trusted_connection=yes;'
        )
        try:
            con = pyodbc.connect(con_str)
            return con
        except:
            print('Connection to `{db}` failed.')
    elif db.lower() == 'db_2023_sa':
        try:
            con = sa.create_engine(assets.SACXN_STR)
            return con
        except:
            print('Connection to `{db}` failed.')

def _exec_qry(con:pyodbc.connect, qry:str) -> pd.DataFrame:
    with open(f'src/qry/{qry}.sql', 'r') as query:
        df = pd.read_sql_query(query.read(),con)
    return df

