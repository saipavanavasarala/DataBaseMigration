'''
1) Get data from oracle db using below query: 

SELECT l.FKID scansubclassificationid,l.scanname scanname,l.SLNO ,s.SUBCLASSIFICATIONNAME ,s2.PKID scanclassificationid,l2.LOCNM ,
s2.LOCID scanclassificationlocation,s.LOCID scansubclassificationlocation,t.TESTNM ,t.TCODE  FROM admin.SCANSUBCLASSIFICATIONDTL l 
JOIN admin.SCANSUBCLASSIFICATION s ON l.FKID =s.PKID  
JOIN admin.SCANCLASSIFICATIONMASTER s2 ON s.SCANCLASSIFICATION = s2.pkid  
JOIN DIAGNOTECH.TESTMAST t ON t.TESTID = l.SCANNAME 
JOIN ip.LOCATIONMAST l2 ON l2.LOCID =s2.LOCID ;

2) Insert into pgadmin uisng below query

insert into "ScanTestMaster"("ScanClassificationId","Duration","ScanTestName","ScanTestCode","CreatedBy","CreatedDate")
select "ScanClassificationId",10,'AFI','AFI',6776,now() from "ScanClassification" where  
"ShivamScanClassificationId"='SC000006'

'''
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

conn1 = psycopg2.connect(
    database="shivam_test_DB",
    user='postgres',
    password='emr123',
    host='192.168.8.97',
    port= '5432'
    )
oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConn = oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConn= postgresdb.connect()



oracleQuery=r'''

SELECT s.SUBCLASSIFICATIONNAME ,
s2.PKID scanclassificationid,t.DURATION, 
t.TESTNM scantestname,t.TCODE scantestcode,6776 createdby,current_date createddate
  FROM admin.SCANSUBCLASSIFICATIONDTL l 
JOIN admin.SCANSUBCLASSIFICATION s ON l.FKID =s.PKID  
JOIN admin.SCANCLASSIFICATIONMASTER s2 ON s.SCANCLASSIFICATION = s2.pkid  
JOIN DIAGNOTECH.TESTMAST t ON t.TESTID = l.SCANNAME 
JOIN ip.LOCATIONMAST l2 ON l2.LOCID =s2.LOCID  ORDER BY  t.testnm asc

'''

oracleRecords=pd.read_sql(oracleQuery.replace("\n",""),oracleConn)

oracleRecords
for i in oracleRecords.to_dict('records'):
    #print(i)
    scanClassificationid=i['scanclassificationid']
    duration=i['duration']
    scantestname=i['scantestname']
    scantestcode=i['scantestcode']
    createdby=i['createdby']
    #createddate=i['createdate']
    
    cursor=conn1.cursor()
    pgadminQuery=f'''  
    insert into "ScanTestMaster"("ScanClassificationId","Duration","ScanTestName","ScanTestCode","CreatedBy","CreatedDate")
    select "ScanClassificationId",{duration},'{scantestname}','{scantestcode}',6776,now() from "ScanClassification" where  
    "ShivamScanClassificationId"='{scanClassificationid}' returning "ScanTestMasterId"
    
    '''
    
    cursor.execute(pgadminQuery.replace("\n",""))
    data=cursor.fetchall()
    print(data)
    conn1.commit()
