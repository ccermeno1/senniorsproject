
from .data import extract_data,transform_data,insert_data,extract_features
from .model import model_predict
from fastapi import FastAPI
import uvicorn
import pandas as pd
from fastapi.responses import HTMLResponse
import copy


app = FastAPI(title="Show up prediction", description="API to predict if a patient will show up", version="1.0")


@app.get('/model/prediction')
async def get_prediction(PatientId: float,AppointmentID:int,Gender:str,ScheduledDay:str,AppointmentDay:str,
Age:int,Neighbourhood:str,Scholarship:int,Hipertension:int,Diabetes:int,Alcoholism:int,Handcap:int,SMS_received:int):
    try:

        df=pd.DataFrame([[PatientId,AppointmentID,Gender,ScheduledDay,AppointmentDay,Age,Neighbourhood,Scholarship,Hipertension,Diabetes,Alcoholism,Handcap,SMS_received]],
        columns=['PatientId','AppointmentID','Gender','ScheduledDay','AppointmentDay','Age','Neighbourhood',
        'Scholarship','Hipertension','Diabetes','Alcoholism','Handcap','SMS_received'])
        transform_data(df)
        df_tr=copy.copy(df)
        df=extract_features(df)
        results= model_predict(df)
        df_tr['No_show prediction']=results
        insert_data(df_tr,'New_InstancesT')
        return {"Prediction": str(results)}
    except Exception as e:
        return {"Error": str(e)}

@app.get("/model/db" ,response_class=HTMLResponse)
async def handle_df():
    try:
        cnames=['PatientId','AppointmentID','Gender','ScheduledDay','AppointmentDay','Age','Neighbourhood',
        'Scholarship','Hipertension','Diabetes','Alcoholism','Handcap','SMS_received','TimeSchedule','No_show prediction']
        df=extract_data(cnames,'New_InstancesT')
        htmlTable= df.to_html()
        return htmlTable
    except Exception as e:
        return {"prediction": str(e)}

if __name__ == "__main__":
    uvicorn.run(app,host='127.0.0.1',port=80)