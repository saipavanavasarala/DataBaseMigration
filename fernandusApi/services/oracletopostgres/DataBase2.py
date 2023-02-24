import cx_Oracle
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime, timedelta
import random
import uuid
from generator import generatePatientFamilyData


fixed_digits = 6 
class oracleHelper:
    def __init__(self):
        self.oracleData=[]
        self.postgresData=[]
        self.oracleRegId=[]
        self.postgrescolumns=[]
        self.postgresREGNO=[]
        self.oracleDatacolumn = []

        self.start = (datetime.now() - timedelta(hours=0, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        #self.start = start.strftime("%Y-%m-%d %H:%M:%S")
        self.date=datetime.now().strftime("%Y-%m-%d")
        self.end=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.oracleQuery=f" SELECT *  FROM OTS1.PATIENTSREGISTRATION  WHERE  REGDT >= to_timestamp('{self.start}', 'YYYY-MM-DD" "HH24:MI:SS')"
        #+f" AND REGDT < to_timestamp('{self.end}', 'YYYY-MM-DD" "HH24:MI:SS')"
        
        
    def getOracleData(self):
        print(datetime.today())
        oracledb = create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
        oracleConn = oracledb.connect()

        df=pd.read_sql(self.oracleQuery,oracleConn)  
        self.oracleData=df
        print("\n the oracle data is :")
        print(df)
        print("the oracle data")
        oracleConn.close()
        
        
    def getPostgresData(self):
        postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
        postgresConn= postgresdb.connect()
        
        df=pd.read_sql(f'SELECT "REGNO" FROM "Patient" where "REGNO" is not NUll',postgresConn)
        self.postgresREGNO=df.values
        
        if len(df)>0:
            data=[element[0] for element in df.values]
            self.postgresREGNO=data
            
        postgresConn.close()
        return df
            
        
       
        
    def appendDataToPostgres(self):
        postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB')
        postgresConn= postgresdb.connect()
        
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
        if len(newDf)<=0:
            return 0
        
        processLog={
            "tableName":"postgres_Patient",
            "numRecords":len(newDf),
            "startDate":datetime.now(),
            "endDate":datetime.now()
        }
            
            
        processLog=pd.DataFrame([processLog.values()],columns=processLog.keys())
        processLog.to_sql("process_log",postgresConn,if_exists="append",index=False)
    
    
        for i in range(len(newDf)):
            oracleDict=newDf.iloc[i].to_dict()
            gender=0 

            dictt={

                "FirstName":oracleDict["FNAME"],
                "MiddleName":oracleDict['MNAME'],
                "LastName":oracleDict['LNAME'],
                "FullName":oracleDict["FNAME"],
                "DateOfBirth":oracleDict['DOB'],
                "Age":oracleDict['AGE'],
                "Gender":gender,
                "Email":oracleDict['EMAIL'],
                "Mobile":oracleDict['MOBILE'],
                "StreetAddress":oracleDict["STREET"],
                "AddressLine2":oracleDict["ADDR2"],
                "City":oracleDict['CITY'],
                "State":oracleDict['STATE'],
                "Zipcode":oracleDict['PINCODE'],
                "CountryId":oracleDict['CITYID'],
                "CreatedDate":oracleDict["REGDT"],
                "AadharNo":oracleDict['AADHARCARDNO'],
                "FatherOrHusband":oracleDict['FATHERNAMEVF'],
                "LocationId":oracleDict['CITYID'],
                "Education":oracleDict['EDUCATION'],
                "Occupation":oracleDict['OCCUPATION'],
                "Religion":oracleDict["RELIGION"],
                "Nationality":oracleDict['NATIONALITY'],
                "IdProofValue":oracleDict['IDPROOF'],
                "IdProofId":oracleDict['IDPROOFID'],
                "BloodGroup":oracleDict['BLOODGROUP'],
                "PayTypeId":oracleDict["PAYTYPE"],
                "REGID":oracleDict["REGID"],
                "REGNO":int(oracleDict['REGNO']),
                "UMRNo":int(oracleDict['REGNO']),


                "Salutation":"",
                "ProfileImageUrl":"",
                "ThumbnailUrl":"",
                "Active":True,
                "CreatedBy":0,
                "Mobile":random.randrange(111111, 999999, fixed_digits) if oracleDict["PHONE2"] is None else oracleDict["PHONE2"],
                "Guid":uuid.uuid1().hex,
                "CountryId":1

 
            }

            postgresEqual=pd.DataFrame([dictt.values()],columns=dictt.keys())
            
            
            #print(postgresEqual,type(postgresEqual))
            try:

                postgresEqual.to_sql('Patient', postgresConn,if_exists= 'append',index=False)
                p_id = pd.read_sql(f'select "PatientId" from "Patient" where "REGNO" = {oracleDict["REGNO"]}',postgresConn).values
                print("The patientid  is : ",p_id[0][0]) 

                patientFamily=generatePatientFamilyData(p_id[0][0],oracleDict=oracleDict)
                #print(type(patientFamily))
                patientFamily.to_sql("PatientFamily",postgresConn,if_exists="append",index=False)


                ##INSERT

            except Exception as e:
                print("the exception is : ",e)
                break
                
            
        postgresConn.close()



        
    def appendToAccount(self):
        start=(datetime.now()-timedelta(hours=0,minutes=30)).strftime("%Y-%m-%d %H:%M:%S")

        end=datetime.now().strftime("%y-%m-%d %H:%M:%S")

        postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB')
        postgresConn= postgresdb.connect()

        df=pd.read_sql(f''' select * from "Patient" where "CreatedDate"='{datetime.now().strftime("%Y-%m-%d")}'  ''',postgresConn)
        df=df.to_dict('records')
        accountRegId=pd.read_sql(f'''select "REGID" from "Account" where "CreatedDate">'{start}' and "CreatedDate"<'{end}' ''',postgresConn)
        accountRegId=accountRegId.to_dict('records')
        try:
            if len(accountRegId)>0:
                accoutRegId=accountRegId[0]['REGID']
        except:
            print(accountRegId)
        
        #print(df)
        
        for model in df:
            try:
                if model["REGID"] not in accountRegId:
                    dictt={
                        "RoleId":4,
                        "ReferenceId":model["PatientId"],
                        "Email":model["Email"],
                        "CountryId":1,
                        "Mobile":model["Mobile"],
                        "FullName":model["FullName"],
                        "IsLocked":False,
                        "Guid":model["Guid"],
                        "CreatedDate":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "REGNO":model["REGNO"],
                        "REGID":model["REGID"]
                    }

                newDf=pd.DataFrame([dictt.values()],columns=dictt.keys())
                newDf.to_sql("Account",postgresConn,if_exists="append",index=False)
            except:
                pass

        
        
        
o=oracleHelper()
o.getOracleData()
o.getPostgresData()
o.appendDataToPostgres()
o.appendToAccount()



