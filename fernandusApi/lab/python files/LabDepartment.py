## LabDepartment

from sqlalchemy import create_engine
import pandas as pd

oracledb=create_engine('oracle://admin:shivam@111.93.11.121:1521/neosoft', echo=False)
oracleConnection=oracledb.connect()

postgresdb=create_engine(r'postgresql+psycopg2://postgres:emr123@192.168.8.97/shivam_test_DB',echo=False)
postgresConnection=postgresdb.connect()

oracleQuery=r'''SELECT distinct(DEPTNM),deptid,6776 createdby,CURRENT_DATE  createddate,
'True' active FROM DIAGNOTECH.DEPARTMENTS d'''.replace("\n","")

oracleData=pd.read_sql(oracleQuery,oracleConnection)

oracleData.rename(columns={
    "active":"Active","deptnm":"DepartmentName","deptid":"DEPTID","createdby":"CreatedBy","createddate":"CreatedDate"
},inplace=True)
#inserting in pgadmin 

oracleData.to_sql("LabDepartment",postgresConnection,if_exists='append',index=False)
