import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
from sklearn import preprocessing

def extract_data(cols,table):
    try:
        cmx=mysql.connector.connect(user='root',password='senniors123',host='34.175.103.60',database='Patients')
        cursor=cmx.cursor()
        cursor.execute('select * from '+ table)
        df=pd.DataFrame(cursor.fetchall(),columns=cols)   
    except Exception as e:
        print('Error: '+str(e))
    finally:
        cursor.close()
    return df

def transform_data(df):
    df['PatientId'] = df['PatientId'].apply(lambda x: str(int(x)))
    df['AppointmentID'] = df['AppointmentID'].apply(lambda x: str(int(x)))
    df.drop(df[df['Age']<0].index,inplace=True)
    df.drop(df[df['Handcap'] > 1].index,inplace=True)
    df['TimeSchedule'] =pd.to_datetime(df['ScheduledDay']).dt.time
    df['ScheduledDay'] =  pd.to_datetime(df['ScheduledDay']).dt.normalize()
    df['AppointmentDay'] = pd.to_datetime(df['AppointmentDay'])
    df.drop(df[(df['AppointmentDay'] - df['ScheduledDay']).dt.days<0].index,inplace=True)
    if 'No-Show' in df:
        df.columns = df.columns.str.replace('No-Show', 'No_Show')
    return df

def insert_data(df,tablename):
    try:
        con=create_engine('mysql+mysqldb://root:senniors123@34.175.103.60/Patients')
        df.to_sql(con=con,name=tablename,if_exists='append',index=False)
    except Exception as e:
        print('Error: '+str(e))

def extract_query(table):
    try:
        cmx=mysql.connector.connect(user='root',password='senniors123',host='34.175.103.60',database='Patients')
        cursor=cmx.cursor()
        cursor.execute(table)
        (number_of_rows,)=cursor.fetchone()
    except Exception as e:
        print('Error: '+str(e))
    finally:
        cursor.close()
    return number_of_rows

def fun_encode(lab):
    le = preprocessing.LabelEncoder()
    y=le.fit_transform(lab)
    return y

def extract_features(df):
    df['ScheduledDayWeek'] = df['ScheduledDay'].dt.day_name()
    df['ScheduledMonth'] = df['ScheduledDay'].dt.strftime("%B")
    df['AppointmenDayWeek'] = df['AppointmentDay'].dt.day_name()
    df['AppointmentMonth'] = df['AppointmentDay'].dt.strftime("%B")
    df['DayDifference'] = (df['AppointmentDay'] - df['ScheduledDay']).dt.days 
    df['TimeSchedule'] = df['TimeSchedule'].apply(lambda x: str(x)).apply(lambda x: str(x).split(':')).apply(lambda x: int(x[0])*3600+int(x[1])*60+int(x[2]))
    df['Gender']=fun_encode(df['Gender'])
    df['Neighbourhood']=fun_encode(df['Neighbourhood'])
    df['ScheduledDayWeek']=fun_encode(df['ScheduledDayWeek'])
    df['ScheduledMonth']=fun_encode(df['ScheduledMonth'])
    df['AppointmenDayWeek']=fun_encode(df['AppointmenDayWeek'])
    df['AppointmentMonth']=fun_encode(df['AppointmentMonth'])

    if 'No_Show' in df:

        df=df.sort_values(by="ScheduledDay",ascending=True)
        df=df.reset_index().drop('index',axis=1)
        df['No_Show']=fun_encode(df['No_Show'])
        df['PreviousApp'] = df.sort_values(by = ['PatientId','AppointmentDay']).groupby(['PatientId']).cumcount()
        df['PreviousMissed'] = df.sort_values(['AppointmentDay']).groupby(['PatientId'])['No_Show'].cumsum()
        df['PreviousMissed'] = df.sort_values(by = ['PatientId','AppointmentDay']).groupby(['PatientId'])['PreviousMissed'].shift().fillna(0)

    else:
        df['PreviousApp'] =extract_query('select count(*) from Instances where PatientId='+ "%s" %(df['PatientId'].loc[0]))
        df['PreviousMissed'] =extract_query('select count(*) from Instances where No_show= "Yes" AND  PatientId='+ "%s" %(df['PatientId'].loc[0]))
        df=df.drop('AppointmentID',axis=1).drop('PatientId',axis=1).drop('ScheduledDay',axis=1).drop('AppointmentDay',axis=1).drop('Neighbourhood',axis=1).drop('ScheduledMonth',axis=1).drop('AppointmentMonth',axis=1)
    return df

