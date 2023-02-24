##ScanSubClassification

'''
pgadminQueries :

1) alter table "ScanSubClassification" add column "ShivamScanClassificationId" varchar(200)

2) select "ScanClassificationId","ShivamScanClassificationId" from "ScanClassification" where "ShivamScanClassificationId" is not null 


oracleQueries : 
 1) SELECT SUBCLASSIFICATIONNAME ,(SELECT SCANCLASSIFICATION FROM admin.SCANCLASSIFICATIONMASTER s 
 WHERE s.PKID =ss.SCANCLASSIFICATION) shivamscanclassificationname,ss.SCANCLASSIFICATION,6776 createdby,
 CURRENT_DATE createddate 
 FROM ADMIN.SCANSUBCLASSIFICATION  ss WHERE ss.SCANCLASSIFICATION ='{ShivamScanClassificationId}'

'''

import pandas as pd
from sqlalchemy import create_engine


oracledb=create_engine(r'oracle://admin:shivam@111.93.11.121:1521/neosoft',echo=False)
postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.98/Test_Shivam_Lab',echo=False)


postgresconn=postgresdb.connect()
oracleConn=oracledb.connect()
postgresquery=f''' select "ScanClassificationId","ShivamScanClassificationId" from "ScanClassification" where "ShivamScanClassificationId" is not null '''

postgresdata=pd.read_sql(postgresquery,postgresconn)

postgresdata=postgresdata.to_dict('records')

for i in postgresdata:
    ScanClassificationId=i['ScanClassificationId']
    ShivamScanClassificationId=i['ShivamScanClassificationId']
    
    oracleDataQuery=f'''SELECT SUBCLASSIFICATIONNAME ,(SELECT SCANCLASSIFICATION FROM admin.SCANCLASSIFICATIONMASTER s WHERE s.PKID =ss.SCANCLASSIFICATION) shivamscanclassificationname,ss.SCANCLASSIFICATION,6776 createdby,CURRENT_DATE createddate  FROM ADMIN.SCANSUBCLASSIFICATION  ss WHERE ss.SCANCLASSIFICATION ='{ShivamScanClassificationId}' '''
    
    oracleData=pd.read_sql(oracleDataQuery,oracleConn)
    oracleData['Active']=[True]*len(oracleData)
    oracleData['ScanClassificationId']=ScanClassificationId
    oracleData.rename(columns={
        'subclassificationname':"ScanSubClassificationName",
        'shivamscanclassificationname':"ShivamScanClassificationId",
        'scanclassification':"ShivamScanClassificationId",
         'createdby':"CreatedBy",
        "createddate":"CreatedDate"
    },inplace=True)
    oracleData.to_sql("ScanSubClassification",postgresconn,if_exists='append',index=False)
   
    print(oracleData.to_dict('records'))
