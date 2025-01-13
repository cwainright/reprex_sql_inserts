"""Connect to databases"""
import assets
import pyodbc
import sqlalchemy as sa
import pandas as pd

def _db_connect(db:str) -> None:

    if db.lower() == 'db_2023':
        try:
            con = sa.create_engine(assets.SACXN_STR)
            return con
        except:
            print('Connection to `{db}` failed.')
    else:
        print('The connection you requested is not in the assets file. Try another connection.')
        return None

def _exec_qry(con:sa.Engine, qry:str) -> pd.DataFrame:
    with open(f'src/qry/{qry}.sql', 'r') as query:
        df = pd.read_sql_query(query.read(),con)
    return df

