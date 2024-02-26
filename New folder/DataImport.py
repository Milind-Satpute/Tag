# -*- coding: utf-8 -*-
"""
Created on Fri Nov  3 13:34:05 2023

@author: satpum
"""
import json;
import pandas as pd
import time 


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

LogsDF=pd.DataFrame(logArray)
print(LogsDF)


fileName='Final_Dpid_Messages_'+ time.strftime("%Y%m%d-%H%M%S")+'.json'
pathName='../Tagging/Data/Output/final/'+fileName
LogsDF.to_json(pathName,orient='records')


# print('=======Finish Saving at '+pathName+'============')

