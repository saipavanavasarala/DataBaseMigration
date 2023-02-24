#LabSampleType
from sqlalchemy import create_engine
import pandas as pd

oracledb=create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConnection=oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConnection=postgresdb.connect()


oracleSamples=pd.read_sql(r"SELECT DISTINCT(SAMPLENM) FROM DIAGNOTECH.TESTMAST t WHERE SAMPLENM  IS NOT NULL",oracleConnection)
oracleSamples['ShivamSample']='y'

oracleSamples.rename(columns=
                    {"samplenm":"TypeName"},inplace=True
                    )
oracleSamples.to_sql("LabSampleType",postgresConnection,if_exists='append',index=False)
