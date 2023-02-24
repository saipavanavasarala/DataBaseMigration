##LabParameterDetail

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

ids=pd.read_sql(f'select  "LabParameterHeaderId","SUBCOMPID" from "LabParameterHeader" where "SUBCOMPID" is not null ',postgresConn)
ids=ids.to_dict('records')

for i in ids:
    try:
        cursor=conn1.cursor()
        #print(i['LabParameterHeaderId'],i['SUBCOMPID'])

        records=pd.read_sql(f'''SELECT DISTINCT INVORDER,TO_number(fromage) as 
                            fromage,TO_Number(toage) as toage,DESC_R ,tp1,tp2,SEX,
                            TRIM(UNIT) AS unitid,CASE WHEN REFRANGETO='.' THEN '0' ELSE REFRANGETO END AS REFRANGETO,
                            CASE WHEN MAXRAFRANGE='.' THEN '0' ELSE MAXRAFRANGE END AS MAXRAFRANGE ,MINCRITVAL ,
                            MAXCRITVAl,COMPID   FROM DIAGNOTECH.MCDHNORMALVALUE1 WHERE COMPID = '{i['SUBCOMPID']}' '''.replace("\n",""),oracleConn)
        records['LabParameterHeaderId']=[i['LabParameterHeaderId']]*len(records)
        unitname=records.to_dict()['unitid'][0]
        
        
            
            
        cursor.execute(f"select * from function_insert_lookupvalue('{unitname}') as id",postgresConn)
        conn1.commit()
        data=cursor.fetchall()
        
        #unitid=int(data[0][0])
        
        
        
        
        
        
        if unitname=='None' or unitname==None or unitname=='null':
            records.drop('unitid', axis=1, inplace=True)
#             records['unitid']=[None]*len(records)
            #print(records.to_dict('records'))
        else:
            records.drop('unitid', axis=1, inplace=True)
            records['unitid']=[unitid]*len(records)
            
        
        
        records.rename(columns={"desc_r":"RangeText","tp1":"FromAgeType","tp2":"ToAgeType",
                                "unitid":"UnitId","refrangeto":"MinValue","maxrafrange":"MaxValue",
                               "mincritval":"MinCriticalValue","maxcritval":"MaxCriticalValue",
                               "compid":"SUBCOMPID","invorder":"InvOrder","sex":"Gender","fromage":"FromAge","toage":"ToAge"
                               }, inplace=True)

        
        
        #records.to_sql("LabParameterDetail",postgresConn,index=False,if_exists="append")
        
    except Exception as e:
        print(data)
        print(str(e))
        print(f"\n {i['SUBCOMPID']} ")
        pass

    
