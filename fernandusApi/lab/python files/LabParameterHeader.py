## LabParameterHeader

import pandas as pd
from sqlalchemy import create_engine

oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConn = oracledb.connect()

limit=10
offset=5
query=f'''SELECT DISTINCT t.DISPLAYPARAM ,t.SUBCOMPONENTNM ,t.SUBCOMPID,6776 AS Createdby ,
CASE WHEN (SELECT count(*) FROM DIAGNOTECH.MCDHNORMALVALUE1 v WHERE v.COMPID = t.SUBCOMPID) > 0 THEN 'numeric' ELSE 'text' END AS referenceoutput
 FROM DIAGNOTECH.TESTSUBCOMPONENTS t'''

dataframe=pd.read_sql(query.replace("\n",""),oracleConn)
oracleConn.close()
x=dataframe.to_dict('records')
dataframe.rename(columns={"displayparam":"DisplayName","subcomponentnm":"ParameterName","referenceoutput":"ReferenceOutput","subcompid":"SUBCOMPID","createdby":"CreatedBy"}, inplace=True)
dataframe['Active']=[True]*len(dataframe)
postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConn= postgresdb.connect()
dataframe.to_sql("LabParameterHeader",postgresConn,if_exists="append",index=False)
postgresConn.close()
