import pandas as pd
from sqlalchemy import create_engine

oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConn = oracledb.connect()

limit=10
offset=5
query=f''' SELECT COMPID ,COMPONENTNM ,COMPID AS shivamcomponentid,6776 AS createdby,CURRENT_DATE AS createddate   FROM DIAGNOTECH.TESTCOMPONENTS'''

dataframe=pd.read_sql(query.replace("\n",""),oracleConn)
oracleConn.close()
x=dataframe.to_dict('records')
x
dataframe['active']=[True]*len(dataframe)
dataframe.rename(columns={
    "compid":"ComponentId","componentnm":"ComponentName","shivamcomponentid":"ShivamComponentId",
    "createdby":"CreatedBy","createddate":"CreatedDate","active":"Active"
    },inplace=True)
postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConn= postgresdb.connect()
dataframe.to_sql("LabComponentHeader",postgresConn,if_exists="append",index=False)
postgresConn.close()
