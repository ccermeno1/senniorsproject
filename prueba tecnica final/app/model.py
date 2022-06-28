
from imblearn.under_sampling import RandomUnderSampler
from sklearn.feature_selection import SelectKBest, mutual_info_classif
import xgboost as xgb
from imblearn.pipeline import Pipeline as imbpipeline
from sklearn.model_selection import cross_val_score
import joblib
from sklearn import preprocessing
import os
import pickle


model=joblib.load('/code/app/final_model.pkl')

def model_predict(df):
    pred_probs=model.predict_proba(df)
    results=pred_probs[:,1][0]
    return results
