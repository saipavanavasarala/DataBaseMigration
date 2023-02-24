##LabTemplateDetails

import psycopg2
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

oracledb=create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleconnection=oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresconn=postgresdb.connect()


postgresdata=pd.read_sql(r'select "SOURCEID","LabTemplateHeaderId" from "LabTemplateHeader" where "SOURCEID" IS NOT NULL',postgresconn)

for i in postgresdata.to_dict('records'):
    try:
        sourceId=i['SOURCEID']
        LabTemplateHeaderId=i['LabTemplateHeaderId']

        oracleData=pd.read_sql(f"SELECT COMPID,CASE when SRORDER IS NULL then 0 ELSE SRORDER END SRORDER   FROM DIAGNOTECH.MCDHNORMALVALUE2 m WHERE SOURCEID='{sourceId}'",oracleconnection)
        for j in oracleData.to_dict('records'):
            compid=j['compid']
            srorder=j['srorder']

            #print(j)
            #print("\n")
            cursor=conn1.cursor()
            query1=f''' insert INTO "LabTemplateDetail"("LabTemplateHeaderId","LabComponentHeaderId","LabParameterHeaderId","SOURCEID","Priority") 
             select "LabTemplateHeaderId",
            CASE WHEN EXISTS(SELECT * FROM "LabComponentHeader" where "ShivamComponentId"='{compid}') THEN "LabComponentHeaderId" ELSE NULL END  "LabComponentHeaderId",
            CASE WHEN EXISTS (SELECT * FROM "LabParameterHeader" where "SUBCOMPID"='{compid}') THEN "LabParameterHeaderId" ELSE NULL END "LabParameterHeaderId",'{sourceId}',{srorder}  from "LabTemplateHeader","LabComponentHeader","LabParameterHeader"
            where "SOURCEID"='{sourceId}' and "SUBCOMPID"='{compid}' OR "ShivamComponentId"='{compid}' LIMIT 1 returning "LabTemplateDetailId"
            '''


            cursor.execute(query1.replace("\n",""))
            conn1.commit()
            data=cursor.fetchall()
            print(data)
        print("\n")
    except Exception as e:
        print(sourceId)
        print(str(e))
        pass
        




