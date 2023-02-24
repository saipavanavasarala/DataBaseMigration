##lABTEMPLATEHEADER

import pandas as pd
from sqlalchemy import create_engine


oracledb=create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleconnection=oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresconn=postgresdb.connect()

oracle_mcdhnormalval2=pd.read_sql(f'''
SELECT  TESTTEMPLATENM ,ALLMETHOD ,INTERPATION,sourceid,
(CASE WHEN ALLMETHOD IS NULL THEN 'False' ELSE 'True' END) AS ismethod,
(CASE WHEN INTERPATION IS NULL THEN 'False' ELSE 'True' end) AS isinterpretation ,current_date as createddate
 FROM DIAGNOTECH.MCDHNORMALVAL2 m 
'''.replace("\n",""),oracleconnection)

oracle_mcdhnormalval2['Active']=[True]*len(oracle_mcdhnormalval2)
oracle_mcdhnormalval2['CreatedBy']=[6776]*len(oracle_mcdhnormalval2)

oracle_mcdhnormalval2.rename(columns={
    'testtemplatenm':"TemplateName",
    "ismethod":"IsMethod",
    "isinterpretation":"IsInterpretation",
    "allmethod":"MethodText",
    "interpation":"InterpretationText",
    "createddate":"CreatedDate",
    "sourceid":"SOURCEID"
    
},inplace=True)
oracle_mcdhnormalval2

#inserting into postgres LabTemplateHeader table

oracle_mcdhnormalval2.to_sql(r"LabTemplateHeader",postgresconn,if_exists="append",index=False)

