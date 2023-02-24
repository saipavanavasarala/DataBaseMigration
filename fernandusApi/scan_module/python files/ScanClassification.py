##ScanClassification
'''
pgadminQueries:

1) alter table "ScanClassification" add column "ShivamScanClassificationId" varchar(200)

Oracla Queries:
1) SELECT SCANCLASSIFICATION,PKID,6776 createdby,CURRENT_DATE createddate FROM admin.SCANCLASSIFICATIONMASTER s


'''

import pandas as pd
from sqlalchemy import create_engine

oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConn = oracledb.connect()
postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.98/Test_Shivam_Lab',echo=False)
postgresConn= postgresdb.connect()

query=f'''SELECT SCANCLASSIFICATION,PKID,6776 createdby,CURRENT_DATE createddate FROM admin.SCANCLASSIFICATIONMASTER s'''
dataframe=pd.read_sql(query.replace("\n",""),oracleConn)
oracleConn.close()
dataframe['Active']=[True]*len(dataframe)
dataframe.rename(columns={
   'scanclassification':"ScanClassificationName",
    "pkid":"ShivamScanClassificationId",
    "createdby":"CreatedBy",
    "createddate":"CreatedDate"
    },inplace=True)

x=dataframe.to_dict('records')
dataframe.to_sql("ScanClassification",postgresConn,if_exists="append",index=False)
postgresConn.close()
