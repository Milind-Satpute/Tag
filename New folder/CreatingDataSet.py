# -*- coding: utf-8 -*-
"""
Created on Sat Nov  4 23:44:31 2023

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

uniquiDpid=[]
uniquiDpid=logsDF['dpid'].unique();



print('============Creating Data Set ===============')
message=''
messageArray=[]
for dpid in uniquiDpid:
    fileName=dpid+'.json'
    directoryName='./Data/Output/Final/Dpids/'+fileName
    print('=======Getting Data From '+directoryName+'============')
    f1  = open(directoryName, encoding="utf8")
    logs = json.load(f1)
    logsDF=pd.DataFrame(logs,columns=['logs'])
    print(logsDF)

    logarray=[]
    for index, row in logsDF.iterrows():
        print(row)
        message=message +row;
    messageArray.append(message)
    print(len(messageArray))


print('============Done creating MessageArray ===============')

LogsDataSetDF=pd.DataFrame(messageArray)

fileName='Final_Log_DataSet'+ time.strftime("%Y%m%d-%H%M%S")+'.json'
pathName='../Tagging/Data/Output/final/'+fileName

print('============Saving DataSet at '+fileName+'====================')

LogsDataSetDF.to_json(pathName,orient='records')

print('============Finish Saving DataSet ===============')
