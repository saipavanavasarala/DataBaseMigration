import cx_Oracle
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime, timedelta
import random
import uuid 



class Boiler:
    def __init__(self):
        self.oracleData=[]
        self.postgresData=[]
        self.oracleRegId=[]
        self.postgrescolumns=[]
        self.postgresREGNO=[]



        self.start = (datetime.today() - timedelta(hours=0, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        #self.start = start.strftime("%Y-%m-%d %H:%M:%S")
        
        self.end=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #self.oracleQuery=f" SELECT *  FROM OTS1.PATIENTSREGISTRATION  WHERE REGDT > to_timestamp('2022-11-20 10:00:00', 'YYYY-MM-DD" "HH24:MI:SS')"+f" AND REGDT < to_timestamp('{self.end}', 'YYYY-MM-DD" "HH24:MI:SS')"
        
        
    def getOracleData(self,oracleQuery):
        oracledb = create_engine('oracle://admin:shivam@183.82.47.239:1521/neosoft', echo='debug')
        oracleConn = oracledb.connect()

        df=pd.read_sql(oracleQuery,oracleConn,)
        self.oracleData=df
        oracleConn.close()
        return df
        
        
    def getPostgresData(self,postgresQuery):
        postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.98/HIMS_backup23-09-2022')
        postgresConn= postgresdb.connect()
        
        df=pd.read_sql(postgresQuery,postgresConn)
        self.postgresREGNO=df.values
        
        if len(df)>0:
            data=[element[0] for element in df.values]
            self.postgresREGNO=data
            
        postgresConn.close()
        return df


    def getNewDataFrame(self):
        temp=[]

        for i in range(len(self.oracleData)):
            j=self.oracleData.iloc[i]
            if int(j['regno']) not in self.postgresREGNO:
                temp.append(list(j.values))
            else:
                pass

        newDf=pd.DataFrame(temp,columns=self.oracleData.columns)
        newDf.columns = [x.upper() for x in newDf.columns]
        print("the length of newdf is ",len(newDf))
        return newDf



    def addToPatientFamily(self,oracledictt,p_id):
        postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.98/HIMS_backup23-09-2022')
        postgresConn= postgresdb.connect()

        dictt={
                


        }




    def appendDataToPostgres(self,newDf,processLog,label2label):
        postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.98/HIMS_backup23-09-2022')
        postgresConn= postgresdb.connect()
        
        # temp=[]

        # for i in range(len(self.oracleData)):
        #     j=self.oracleData.iloc[i]
        #     if int(j['regno']) not in self.postgresREGNO:
        #         temp.append(list(j.values))
        #     else:
        #         pass

        # newDf=pd.DataFrame(temp,columns=self.oracleData.columns)
        # newDf.columns = [x.upper() for x in newDf.columns]
        # print("the length of newdf is ",len(newDf))
        if len(newDf)<=0:
            return 0

        processLog=pd.DataFrame([processLog.values()],columns=processLog.keys())
        processLog.to_sql("process_log",postgresConn,if_exists="append",index=False)


        for i in range(len(newDf)):
            oracleDict=newDf.iloc[i].to_dict()
            gender=0 if oracleDict["GENDER"] is "FEMALE" else 1
            dictt={}

            for i,j in label2label.items():
                dictt[i]=oracleDict[j]

            postgresEqual=pd.DataFrame([dictt.values()],columns=dictt.keys())
            
            #print(postgresEqual,type(postgresEqual))
            try:

                postgresEqual.to_sql('Patient', postgresConn,if_exists= 'append',index=False)
            except Exception as e:
                pass
                
            
        postgresConn.close()


appointmentTable=Boiler()
postgresrows=appointmentTable.getPostgresData(f'SELECT * FROM "PatientFamily" ')
print(postgresrows)
#appointmentTable.getOracleData('')
#newDf=appointmentTable.getNewDataFrame('')


