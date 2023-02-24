import cx_Oracle
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime, timedelta
import random
import traceback


td=datetime.today().strftime("%Y-%m-%d")

query=f'''SELECT 
di.DOCTORNM doctor_name,ads.SUBCOMPANYNM  specialization,CASE WHEN di.SEX=1 THEN 'M' ELSE 'F' END AS Gender,l.LOCNM AS location_name ,
d.APPTGIVENDT1,d.REGID,d.APPTNO,d.FROMTIME,d.TOTIME,d.DAID,d.TM,(SELECT a.DT FROM OTS1.DOCTORAPPOINTMENTHDR a WHERE d.DAID=a.DAID) AS AppointmentDate 
FROM OTS1.DOCTORAPPOINTMENTDTS d  
JOIN  OTS1.DOCTORAPPOINTMENTHDR hd ON d.DAID =hd.DAID 
JOIN OTS1.DOCTORINFO di ON hd.DOCID = di.DOCID 
JOIN ADMIN.SUBCOMPANIES ads ON hd.SPEID =ads.SUBCOMPANYID 
JOIN IP.LOCATIONMAST l ON hd.LOCID =l.LOCID 
WHERE APPTGIVENDT1 >= to_timestamp('{td}', 'YYYY-MM-DD" "HH24:MI:SS')'''.replace("\n",'')

query2= f'''select "AppointmentNo" from "Appointment" where "REGID" is not null AND "CreatedDate"='{td}' '''

def getOracleData():
    conn1 = psycopg2.connect(
    database="shivam_test_DB",
    user='postgres',
    password='emr123',
    host='192.168.8.97',
    port= '5432'
    )

    cursor=conn1.cursor()
    oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
    postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB', echo=False)
    
    oracleConn = oracledb.connect()
    postgresConn=postgresdb.connect()
    regidslist=[]
    df=pd.read_sql(query,oracleConn)
    df.to_csv("new.csv")
    regids=pd.read_sql(query2,postgresConn)
    if len(regids)>0:
        
        for row in regids.to_dict('records'):
            regidslist.append(str(row["AppointmentNo"]))

    else:
        regids=[]
    
    regids=regidslist
    print(regids)
    
    #print("The df  are :",df)
    oracleData=df.to_dict('records')
    count=0
    for row in oracleData:
        try:
            if str(row['apptno']) not in regids:
                print(row['apptno'],row['apptno'] in regids)
                patientId=pd.read_sql(f''' select "PatientId" from "Patient" where "REGID"='{row["regid"]}'  ''',postgresConn)
                
                if len(patientId)>0:
                    patientId=patientId.to_dict('records')[0]["PatientId"]
                    dictt={
                        "PatientId":patientId,
                        
                        "AppointmentNo": row['apptno'] ,
                        "AppointmentTime":row['fromtime'],
                        "AppointmentEndTime":row['totime'],
                        "AppointmentDate":row['appointmentdate'],
                        "REGID":row['regid'],
                        "CreatedDate":row['apptgivendt1'],
                        "DoctorName":"",
                        "LocationName":row['location_name'],
                        "Specialization":row['specialization'],
                        "Gender":row["gender"],
                        
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
                    from_time=row["fromtime"].strftime("%H:%M:%S")
                    to_time=row["totime"].strftime("%H:%M:%S")
                    apptno=str(row['apptno'])
                    print( row['apptno'],row['apptgivendt1'],row['appointmentdate'])
                    cursor.execute(f'''select * from "function_insert_appointments"({patientId},'{apptno}'
                        ,'{row['location_name']}','{row['specialization']}','{row['regid']}','{row['doctor_name']}','{row["gender"]}'
                        ,TO_TIMESTAMP('{from_time}','HH24:MI:SS') ::timestamp,TO_TIMESTAMP('{to_time}','HH24:MI:SS') ::timestamp,
                        TO_DATE('{row['apptgivendt1']}', 'YYYY-MM-DD')::date,TO_DATE('{row['appointmentdate']}', 'YYYY-MM-DD')::date)'''.replace("\n",""))

                    conn1.commit()
                    count=count+1

            

            
               
        except Exception as e:
            #pass
            print("\n the exception is ",traceback.format_exc())
    conn1.close()
    print("inserted rows",count)
        
    
#     oracleConn.close()
#     postgresConn.close()
    return oracleData

print("The program has started")
d=getOracleData()
print("The program has completed")
