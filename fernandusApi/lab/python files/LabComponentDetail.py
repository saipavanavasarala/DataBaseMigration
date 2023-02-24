import pandas as pd
from sqlalchemy import create_engine

oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConn = oracledb.connect()



postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConn= postgresdb.connect()

ids=pd.read_sql(f'select * from "LabComponentHeader" where "ShivamComponentId" is not null ',postgresConn)
ids=ids.to_dict('records')

for i in ids:
    try:
        componentId=i['ShivamComponentId']
        labComponentHeaderId=i['LabComponentHeaderId']
        print("The labcomponentheaderid is : ",labComponentHeaderId)
        records=pd.read_sql(f''' SELECT SNO,COMPONENTID,SUBCOMPONENTID FROM  DIAGNOTECH.ASSIGNSUBCOMPTOCOMP WHERE COMPONENTID = '{componentId}' ''',oracleConn)
        print(records.to_dict("records"))
        for j in records.to_dict("records"):
            priority=j['sno']
            
            subid=j['subcomponentid']
            id2=pd.read_sql(f''' select "LabParameterHeaderId" from "LabParameterHeader" where "SUBCOMPID"='{subid}'  ''',postgresConn)
            #print("The labParameterHeaderId is : ",id2.to_dict('records')[0]['LabParameterHeaderId'])
            dictt={
                "LabComponentHeaderId":labComponentHeaderId,
                "LabParameterHeaderId":id2.to_dict('records')[0]['LabParameterHeaderId'],
                "Priority":priority,
                "ShivamComponentId":componentId,
                "SUBCOMPID":subid
            }
            
            print(dictt)
            df=pd.DataFrame([dictt.values()],columns=dictt.keys())
            df.to_sql("LabComponentDetail",postgresConn,if_exists="append",index=False)
            
            
        print("\n")

    except Exception as e:
        print(str(e))
        #print(f"\n {i['SUBCOMPID']} ")
        pass
