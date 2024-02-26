# -*- coding: utf-8 -*-
"""
Created on Fri Nov  3 13:34:05 2023

@author: satpum
"""
import json;
import pandas as pd
import time 


directoryName='./Data/Output/Final/Final_Dpid_Messages_20231103-225559.json'
print('=======Getting Data From '+directoryName+'============')

f1  = open(directoryName, encoding="utf8")

logs = json.load(f1)
logsDF=pd.DataFrame(logs,columns=['dpid','message','domain']) 
#print(logsDF.head(2))
uniquiDpid=[]
uniquiDpid=logsDF['dpid'].unique();

print('============Saving logs in Dpid Name file ===============')
message=[]

#print(logsDF[(logsDF['dpid']=='7020000346408')]['message'])

for dpid in uniquiDpid:
    fileName=dpid+'.json'
    pathName='../Tagging/Data/Output/final/Dpids/'+fileName
    series=logsDF[(logsDF['dpid']==dpid)]['message']
    #print(type(series))
    pdf=series.to_frame()
#    pdf=pdf.rename(columns = {0:'message'})
    pdf['message'].to_json(pathName,orient='records')
    #logsDF[(logsDF['dpid']==dpid)]['message'].to_json(pathName)
    print('===saving file at '+pathName) 