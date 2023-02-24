from sqlalchemy import create_engine
import pandas as pd
import psycopg2

oracledb=create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConnection=oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConnection=postgresdb.connect()

oracleData=pd.read_sql(f"SELECT TEMPID,TESTID  FROM DIAGNOTECH.TESTTEMPLATES t",oracleConnection)
conn1 = psycopg2.connect(
    database="shivam_test_DB",
    user='postgres',
    password='emr123',
    host='192.168.8.97',
    port= '5432'
    )
for i in oracleData.to_dict('records'):
    try:
        cursor=conn1.cursor()
        sourceid=i['tempid']
        testcode=i['testid']

        pgadminQuery=f''' insert into "LabMainDetailTemplate"("LabMainDetailId","LabTemplateHeaderId") 
    select  "LabMainDetailId","LabTemplateHeaderId"  from 
    "LabTemplateHeader","LabMainDetail" where "SOURCEID"='{sourceid}' and "TestCode"='{testcode}' limit 1 returning "LabMainDetailTemplateId" '''

        cursor.execute(pgadminQuery.replace("\n",""))
        conn1.commit()
        data=cursor.fetchall()
        print(data)
    except:
        pass
conn1.close()
