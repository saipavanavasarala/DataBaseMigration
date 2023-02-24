
'''
pgadminQueries:

1) select * from "ScanMachineTestMap"

2) alter table "ScanMachineTestMap" drop column "LocationId" 

3) alter table "ScanMachineTestMap" add column "Shivam_MACID" VARCHAR(200),ADD COLUMN "Shivam_TESTID" VARCHAR(200)

oracleQueries:

1) SELECT  s.MACID,s.TESTID,t.TCODE  FROM ots1.SCANMACDTL s JOIN DIAGNOTECH.testmast t ON t.TESTID =s.TESTID

'''




import pandas as pd
from sqlalchemy import create_engine
import psycopg2


conn1 = psycopg2.connect(
    database="Test_Shivam_Lab",
    user='postgres',
    password='emr123',
    host='192.168.8.98',
    port= '5432'
    )


oracledb=create_engine(r'oracle://admin:shivam@111.93.11.121:1521/neosoft',echo=False)
postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.98/Test_Shivam_Lab',echo=False)

postgresConn=postgresdb.connect()
oracleConn=oracledb.connect()

oracleQuery =r"SELECT  s.MACID,s.TESTID,t.TCODE  FROM ots1.SCANMACDTL s JOIN DIAGNOTECH.testmast t ON t.TESTID =s.TESTID"

oracleRecords=pd.read_sql(oracleQuery,oracleConn)

cursor=conn1.cursor()
counter=0
for i in oracleRecords.to_dict('records'):
    #print(i)
    macid=i['macid']
    testid=i['testid']
    ScanTestCode=i['tcode']
    pgadminQuery=f'''
    
    insert into "ScanMachineTestMap"("Shivam_MACID","Shivam_TESTID","ScanMachineMasterId","ScanTestMasterId")  
    select '{macid}','{testid}',sm."ScanMachineMasterId",st."ScanTestMasterId" from "ScanMachineMaster" sm ,"ScanTestMaster" st 
    where  sm."ShivamMachineId"='{macid}' and LOWER(st."ScanTestCode")=LOWER('{ScanTestCode}')  LIMIT 1 returning "ScanMachineTestMapId"
    '''
    cursor.execute(pgadminQuery.replace("\n",""))
    data=cursor.fetchall()
    conn1.commit()
    if len(data)>0:
        print(data)
        counter=counter+1
    
    
