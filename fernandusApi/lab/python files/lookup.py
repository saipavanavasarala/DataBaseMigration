#insert into lookuptable

import pandas as pd
from sqlalchemy import create_engine


postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConn= postgresdb.connect()

df=pd.read_sql(f"select * from function_insert_lookupvalue('mg') as id",postgresConn)
