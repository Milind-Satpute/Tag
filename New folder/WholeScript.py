# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 15:56:13 2023

@author: satpum
"""

import json;
import pandas as pd
import time 

print('===============IMPORTING DATA====================')
directoryName='./Data/Input/Ge2_Omcs_Sidecar_October.json'
print('=======Getting Data From '+directoryName+'============')

f1  = open(directoryName, encoding="utf8")


dataSideCarLogs = json.load(f1)

#print(dataSideCarLogs)

sidecarDataFrame= pd.DataFrame(dataSideCarLogs,columns = ['took', 'timed_out', '_shards', 'hits'])


hitsRowArray=[]
for hits in sidecarDataFrame['hits']['hits']:
    hitsRowArray.append(hits)


logprocessedDF= pd.DataFrame(hitsRowArray)

logArray=[]
for log in logprocessedDF['log_processed']:
    logArray.append(log)

logsDF=pd.DataFrame(logArray,columns=['dpid','message','domain'])


print('===============READING HITS ====================')


#logsDF=pd.DataFrame(logs,columns=['dpid','message','domain']) 
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



print('============Creating Data Set ===============')

uniquiDpid=[]
uniquiDpid=logsDF['dpid'].unique();

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
pathName='../Tagging/Data/Output/final/DataSet/'+fileName

print('============Saving DataSet at '+fileName+'====================')

LogsDataSetDF.to_json(pathName,orient='records')

print('============Finish Saving DataSet ===============')




