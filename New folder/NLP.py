# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 14:34:50 2023

@author: satpum
"""

import pandas as pd
import numpy as np
#for text pre-processing
import re, string
import json;




pathName='./Data/Output/Final/Dataset/'
directoryName='./Data/Output/Final/Dataset/Final_Log_DataSet20231105-160326.json'
print('=======Getting Training Data From '+directoryName+'============')

f1  = open(directoryName, encoding="utf8")

logs = json.load(f1)
logsDF=pd.DataFrame(logs) 

#logsDF.to_json(pathName+'/logsDF.json')