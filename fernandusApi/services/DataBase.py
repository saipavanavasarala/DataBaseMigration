
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime

class DataBases:
    def __init__(self):
        self.postgresData=[]
        self.postgresReqNo=[]
        self.oracleReqNo=[]
        self.oracleData=[]
        self.startTime=datetime.now()
        
    def getOracleData(self):
        connection = cx_Oracle.connect(user="admin", password="shivam",
                               dsn="183.82.47.239:1521/neosoft",
                               encoding="UTF-8")
        df=pd.read_sql(f"SELECT * FROM ADMIN.PATIENTPROFILE  ",connection)
        connection.close()
        self.oracleData=df
        oracleReqNo=[element[0] for element in df.values]
        self.oracleReqNo=oracleReqNo
        self.startTime=datetime.now()
        
    
    def getPostGresData(self):
        conn = psycopg2.connect(
        host="localhost",
        database="test2",
        user="postgres",
        password="postgres")
        postgres=pd.read_sql(f'SELECT * FROM "PATIENTPROFILE"',conn)
        self.postgresData=postgres
        postgresreq=[element[0] for element in postgres.values]
        self.postgresReqNo=postgresreq
        #print("the recores in postgres is : ",self.postgresData)
        conn.close()
        
    
    def appendToPostGres(self):
        temp=[]
        
        for i in self.oracleData.values:
            if i[0] not in self.postgresReqNo:
                temp.append(i)
            else:
                pass

        newDf=pd.DataFrame(temp,columns=self.oracleData.columns)
        #print(len(newDf))
        
        if len(newDf)<=0:
            print(f"{len(newDf)} oracle records to upload in postgres")
            return f"no oracle records to upload in postgres  {len(newDf)}"

        
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
                "tablename":"postgres_PATIENTPROFILE",
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


    def appendToOracle(self):
        temp=[]
  
        for i in self.postgresData.values:
            if i[0] not in self.oracleReqNo:
                temp.append(i)
            else:
                pass

        newDf=pd.DataFrame(temp,columns=self.oracleData.columns)
        #print(len(newDf))
        
        if len(newDf)<=0:
            print(f"{len(newDf)} postgresrecords to upload in oracle ")
            return f"no postgresrecords to upload in oracle  {len(newDf)}"

        else:
            print("uploading data")
            oracledb = create_engine('oracle://admin:shivam@183.82.47.239:1521/neosoft', echo='debug')
            postgresdb=create_engine(r'postgresql+psycopg2://postgres:postgres@localhost/test2')

            oracleConn = oracledb.connect()
            postgresConn=postgresdb.connect()


            #oracle=pd.read_sql(f'SELECT * FROM PATIENTPROFILE',oracleConn)
            newDf.to_sql("PATIENTPROFILE", oracledb, if_exists= 'append',index=False)


            processlog={
                "tablename":"oracle_PATIENTPROFILE",
                "numrecords":len(newDf),
                "startdate":self.startTime,
                "enddate":datetime.now(),
            }
            processData=pd.DataFrame([processlog.values()],columns=processlog.keys())
            processData.to_sql("processlog",postgresConn,if_exists='append',index=False)

            oracleConn.close()
            postgresConn.close()
            
h=DataBases()
h.getOracleData()
h.getPostGresData()
h.appendToPostGres()
h.appendToOracle()

print("program runned done")
