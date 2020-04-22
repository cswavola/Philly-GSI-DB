import json
import sqlite3
import re

fname = input('Enter file name: ')
if len(fname) < 1 : fname = 'GSI_Private_Projects_Retrofit.geojson'

with open(fname) as f :
    dict_data = json.loads(f.read())

print(dict_data.keys())

sd = 0
gd = 0

for f in dict_data['features'] :
    project = f['properties']
    #print(project['GRANTAMOUNT'])
    #print(type(project['GRANTAMOUNT']))
    if project['SMIP'] == -1 and project['GRANTAMOUNT'] is not None :
        #print(project['GRANTAMOUNT'], project['NAME'])
        sd = sd + project['GRANTAMOUNT']
    elif project['GARP'] == -1 and project['GRANTAMOUNT'] is not None :
        gd = gd + project['GRANTAMOUNT']
    else : continue

print('SMIP Amount :', sd)
print('GARP Amount :', gd)

#Pretty printing
#print(json.dumps(info, indent = 4, sort_keys = True))
