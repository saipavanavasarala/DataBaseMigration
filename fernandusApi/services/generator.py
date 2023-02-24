import pandas as pd 
from datetime import datetime

def generatePatientFamilyData(p_id,oracleDict):
    print("Enntered in function")
    dictt={
        "PatientId":p_id,
        "FullName":"*" if oracleDict["HUSBANDNM"] is None else oracleDict["HUSBANDNM"],
        "Relation":"**",
        "Relation":"*" if oracleDict["RELATIONWITHPATIENT"] is None else oracleDict["RELATIONWITHPATIENT"] ,
        "Age":oracleDict['HUSBANDAGE'],
        "Gender":oracleDict['RELSEX'],
        "Active":True,
        "CreatedBy":0,
        "CreatedDate":datetime.now(),
        "ModifiedBy":0,
        "ContactNo":oracleDict["HUSMOBILE"],
        "Education":oracleDict["EDUCATION"],
        "Occupation":oracleDict["HUSBOCCUPATION"],
        "DOB":oracleDict["HUSDOB"],
        "REGID":oracleDict["REGID"],
        "REGNO":oracleDict["REGNO"]
    }

    patientFamily=pd.DataFrame([dictt.values()],columns=dictt.keys())
    return patientFamily


