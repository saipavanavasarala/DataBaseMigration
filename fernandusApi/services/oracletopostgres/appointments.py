import cx_Oracle
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime, timedelta
import random


td=datetime.today().strftime("%Y-%m-%d")

query=f"SELECT  APPTGIVENDT1,REGID,APPTNO,FROMTIME,TOTIME,  FROM OTS1.DOCTORAPPOINTMENTDTS WHERE  APPTGIVENDT1 >= to_timestamp('{td}', 'YYYY-MM-DD" "HH24:MI:SS')"
query2= f'''select "AppointmentNo" from "Appointment" where "REGID" is not null AND "AppointmentDate"='{td}' '''

def getOracleData():
    oracledb = create_engine('oracle://admin:shivam@183.82.47.239:1521/neosoft', echo='debug')
    postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB')
    
    oracleConn = oracledb.connect()
    postgresConn=postgresdb.connect()
    regidslist=[]
    df=pd.read_sql(query,oracleConn)
    df.to_csv("new.csv")
    regids=pd.read_sql(query2,postgresConn)
    if len(regids)>0:
        
        for row in regids.to_dict('records'):
            regidslist.append(row["AppointmentNo"])

    else:
        regids=[]
    
    regids=regidslist
    
    #print("The df  are :",df)
    oracleData=df.to_dict('records')
    count=0
    for row in oracleData:
        try:
            if row['apptno'] not in regids:
                patientId=pd.read_sql(f''' select "PatientId" from "Patient" where "REGID"='{row["regid"]}'  ''',postgresConn)
                patientId=patientId.to_dict('records')[0]["PatientId"]
                dictt={
                    "PatientId":patientId,
                    
                    "AppointmentNo": row['apptno'] ,#random.randrange(111111, 999999, 16),
                    "AppointmentTime":row['fromtime'],
                    "AppointmentEndTime":row['totime'],
                    "AppointmentDate":row['apptgivendt1'],
                    "REGID":row['regid'],
                    
                    "IsEncounter":True,
                    "VisitTypeId":8,
                   "ProviderLocationId":0,
                    "ProviderId":403,
                    "LocationId":1,
                    "AppointmentTypeId":1,
                    "QueueStatusId":7,
                    "PayTypeId":1,
                    "SpecializationId":102,
                    "ConsultationTypeId":1,
                    "ProviderAvailabilityId":381,
                    "ChargeTypesId":1,
                    "PaymentStatus":True,
                }

        

                df=pd.DataFrame([dictt.values()],columns=dictt.keys())
                df.to_sql("Appointment",postgresConn,if_exists='append',index=False)
                count=count+1
               
        except Exception as e:
            pass
            #print("the exception is ",e)
    print("inserted rows",count)
        
    
    oracleConn.close()
    postgresConn.close()
    return oracleData

print("The program has started")
d=getOracleData()
print("The program has completed")