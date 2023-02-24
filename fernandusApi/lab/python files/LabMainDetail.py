from sqlalchemy import create_engine
import pandas as pd
import psycopg2

oracledb=create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConnection=oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConnection=postgresdb.connect()

conn1 = psycopg2.connect(
    database="shivam_test_DB",
    user='postgres',
    password='emr123',
    host='192.168.8.97',
    port= '5432'
    )
cursor=conn1.cursor()
LabDepartment=pd.read_sql(r'''select "LabDepartmentId","DEPTID" from "LabDepartment" where "DEPTID" is not null ''',postgresConnection)

LabDepartmentRecords=LabDepartment.to_dict('records')

for i in LabDepartmentRecords:
    LabDepartmentId=i['LabDepartmentId']
    DEPTID=i['DEPTID']
    
    oracleQuery=f'''

    SELECT TESTID,TESTNM,SAMPLENM, 
     CASE WHEN EXISTS(SELECT * FROM DIAGNOTECH.TESTTEMPLATES t WHERE t.TESTID = TESTID) THEN 'True' ELSE 'False' END AS isinternal,
     CASE WHEN EXISTS(SELECT * FROM DIAGNOTECH.TESTTEMPLATES t WHERE t.TESTID = TESTID) THEN 'False' ELSE 'True' END AS isexternal
     FROM DIAGNOTECH.TESTMAST  WHERE DEPTID = '{DEPTID}'   '''
 
    oracle_testmast=pd.read_sql(oracleQuery.replace("\n",""),oracleConnection)
    oracle_testmast['DEPTID']=DEPTID
    #print(oracle_testmast)
    cursor=conn1.cursor()
    for row in oracle_testmast.to_dict('records'):
        testname=row['testnm'].lower()
        testcode=row['testid']
        sampleName=row['samplenm']
        print(testname)
        
        if sampleName=="None" or sampleName ==None:
            pgadminQuery=f'''insert into "LabMainDetail"("TestName","LabDepartmentId","TestCode", "CreatedBy","CreatedDate")
      
        select '{testname}',
        "LabDepartmentId" ,
        '{testcode}',
        6776,now()
        from "LabDepartment","LabSampleType"
         where "DEPTID"='{DEPTID}'  limit 1 returning "LabMainDetailId"
            
            '''
            
        else:
            
            pgadminQuery=f'''insert into "LabMainDetail"("TestName","LabDepartmentId","TestCode",
            "LabSampleTypeId", "CreatedBy","CreatedDate") 
            select '{testname}',
            "LabDepartmentId" ,
            '{testcode}' ,
            "LabSampleTypeId",
            6776,now()
            from "LabDepartment","LabSampleType"
            where "DEPTID"='{DEPTID}' and "TypeName"='{sampleName}' limit 1 returning "LabMainDetailId"
            
            '''
        #print(pgadminQuery.replace("\n",""))
        #print("\n")
        print(DEPTID,testname)
        cursor.execute(pgadminQuery.replace("\n",""))
        conn1.commit()
        data=cursor.fetchall()
        print(data)
        
    
    
    print("\n")
    
    
