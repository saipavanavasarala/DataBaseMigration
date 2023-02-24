#!/usr/bin/env python
# coding: utf-8

# In[64]:



import cx_Oracle
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime, timedelta
#from generator import generatePatientFamilyData
import random

fixed_digits = 6


# In[132]:


number=1456785
class oracleHelper:
    def __init__(self):
        self.oracleDatacolumn = []
        self.postgredata=[]
        self.start = (datetime.today() - timedelta(hours=0, minutes=30)).strftime("%Y-%m-%d")
        # self.start = start.strftime("%Y-%m-%d %H:%M:%S")
        self.start = (datetime.today() - timedelta(hours=0, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        #self.start = start.strftime("%Y-%m-%d %H:%M:%S")
        
        self.end=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.oracleQuery=f" SELECT *  FROM OTS1.PATIENTSREGISTRATION  WHERE REGDT > to_timestamp('2022-11-20 10:00:00', 'YYYY-MM-DD" "HH24:MI:SS')"+f" AND REGDT < to_timestamp('{self.end}', 'YYYY-MM-DD" "HH24:MI:SS')"
        self.end = datetime.now().strftime("%Y-%m-%d ")

    def connectpostgre(self):
        postgresdb = create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/ivf',echo=False)
        postgresConn = postgresdb.connect()
        df = pd.read_sql(f'select * from "Patient" order by 1 desc LIMIT 2', postgresConn)
        self.postgredata=df
        df.to_csv('post.csv')
        postgresConn.close()     
    def connectoracle(self):
        oracledb = create_engine('oracle://admin:shivam@183.82.47.239:1521/neosoft', echo=False)
        oracleConn = oracledb.connect()
        df=pd.read_sql(self.oracleQuery,oracleConn)  
        self.oracleDatacolumn=df
        #df.to_csv('oracle.csv')
        
        oracleConn.close()
    def appendtooracle(self):
        oracledb = create_engine('oracle://admin:shivam@183.82.47.239:1521/neosoft')
        oracleConn = oracledb.connect()
        dict={
                "PatientId":"PATIENTID",
                "FirstName":"FNAME",
                "MiddleName":'MNAME',
                "LastName":'LNAME',
                "Age":'AGE',
                "Gender":'GENDERTYPE',
                "Email":'EMAIL',
                "Mobile":'MOBILE',
                "StreetAddress":"STREET",
                "AddressLine2":"ADDR2",
                "City":'CITY',
                "State":'STATE',
                "Zipcode":'PINCODE',
                "CountryId":'CITYID',
                "Active":'ACTIVE',
                "AadharNo":'AADHARCARDNO',
                "FatherOrHusband":'FATHERNAMEVF',
                "Education":'EDUCATION',
                "Occupation":'OCCUPATION',
                "Religion":"RELIGION",
                "Nationality":'NATIONALITY',
                "IdProofId":'IDPROOFID',
                "BloodGroup":'BLOODGROUP',
                "PayTypeId":"PAYTYPE",
                "REGNO":'REGNO',
               
            }
        self.oracleDatacolumn.columns=[x.upper() for x in self.oracleDatacolumn.columns]
        self.oracleDatacolumn= self.oracleDatacolumn.iloc[0:0]
        self.postgredata.rename(columns=dict,inplace=True)
        col=[]
        for i in dict.values():
            col.append(i)
        self.postgredata=self.postgredata[col]
        self.postgredata['GENDERTYPE'] = self.postgredata['GENDERTYPE'].replace({'M':0,'F':1})
        self.postgredata['ACTIVE'] = self.postgredata['ACTIVE'].replace({True:1,False:0})
        len1=len(self.postgredata)
        REGID=[]
        for i in range(len1):
            
                REGID.append(str(random.randint(10000000, 99999999)))
        self.postgredata['REGID']=REGID
        self.postgredata.fillna('',inplace=True)
        self.postgredata.to_csv(r'post1.csv')

        values=[tuple(x) for x in self.postgredata.values]
#         print(values)
        
#         col2=tuple(self.postgredata)
#         print(col2)
       
        for i in values:
            query=f"INSERT  INTO OTS1.PATIENTSREGISTRATION  (PATIENTID, FNAME, MNAME, LNAME, AGE, GENDERTYPE, EMAIL,MOBILE, STREET, ADDR2, CITY, STATE, PINCODE, CITYID,ACTIVE, AADHARCARDNO,FATHERNAMEVF,EDUCATION,OCCUPATION,RELIGION,NATIONALITY,IDPROOFID,BLOODGROUP,PAYTYPE,REGNO,REGID) VALUES {i}"
            print(query)
            oracleConn.execute(query)
        oracleConn.close()    
            


# In[133]:


o=oracleHelper()
o.connectpostgre()
o.connectoracle()
o.appendtooracle()


# In[ ]:


#TO_DATE('14-SEP-2000', 'DD-MON-YYYY')

