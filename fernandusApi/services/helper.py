import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime

class DataBases:
    def __init__(self):
        self.postgresData=[]
        self.oracleData=[]
        self.startTime=datetime.now()
        
    def getOracleData(self):
        connection = cx_Oracle.connect(user="admin", password="shivam",
                               dsn="183.82.47.239:1521/neosoft",
                               encoding="UTF-8")
        df=pd.read_sql(f"SELECT * FROM ADMIN.PATIENTPROFILE  ",connection)
        connection.close()
        self.oracleData=df
        self.startTime=datetime.now()
        
    
    def getPostGresData(self):
        conn = psycopg2.connect(
        host="localhost",
        database="test2",
        user="postgres",
        password="postgres")
        postgres=pd.read_sql(f'SELECT * FROM "PATIENTPROFILE"',conn)
        postgres=[element[0] for element in postgres.values]
        self.postgresData=postgres
        conn.close()
        
    
    def appendToPostGres(self):
        temp=[]
        
        for i in self.oracleData.values:
            if i[0] not in self.postgresData:
                temp.append(i)
            else:
                pass

        newDf=pd.DataFrame(temp,columns=self.oracleData.columns)

        
        if len(newDf)<=0:
            
            return f"No new records to add  {len(newDf)}"
    
        # establish connections
        else:
            conn_string = r'postgresql+psycopg2://postgres:postgres@localhost/test2'

            db = create_engine(conn_string)
            conn = db.connect()
            conn1 = psycopg2.connect(
                database="test2",
            user='postgres',
            password='postgres',
            host='localhost',
            port= '5432'
            )

            conn1.autocommit = True
            cursor = conn1.cursor()

            ##adding log details
            processlog={
                "tablename":"PATIENTPROFILE",
                "numrecords":len(newDf),
                "startdate":datetime.now(),
                "enddate":datetime.now(),
            }
            processData=pd.DataFrame([processlog.values()],columns=processlog.keys())
            processData.to_sql("processlog",conn,if_exists='append',index=False)

            # adding oracle data to postgres
            newDf.to_sql('PATIENTPROFILE', conn, if_exists= 'append',index=False)

            conn1.commit()
            conn1.close()
            
class AppServerSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "TestService22"
    _svc_display_name_ = "Test Service22"


    def __init__(self,args):
        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None,0,0,None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                          servicemanager.PYS_SERVICE_STARTED,
                          (self._svc_name_,''))
        self.main()

    def main(self):
        
        # Your business logic or call to any class should be here
        # this time it creates a text.txt and writes Test Service in a daily manner 
        f = open('C:\\test.txt', 'a')
        rc = None
        while rc != win32event.WAIT_OBJECT_0:
            f.write('Test Service  \n')
            f.flush()
            # block for 24*60*60 seconds and wait for a stop event
            # it is used for a one-day loop
            rc = win32event.WaitForSingleObject(self.hWaitStop, 24 * 60 * 60 * 1000)
        f.write('shut down \n')
        f.close()

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(AppServerSvc)
