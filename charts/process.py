import csv
from pymongo import MongoClient
import datetime
import json

LIMIT = 1e10

client = MongoClient()
db = client.extractives
coll = db.data
coll2 = db.data2

def convert_float(string):
    if string == '': return 0
    else:
       try:
           return float(string)
       except:
           return 0

cols = [
        {"id":"","label":"Country","pattern":None,"type":"string"},
        {"id":"","label":"Year","pattern":None,"type":"date"},
        {"id":"","label":"Electricity","pattern":None,"type":"number"},
        {"id":"","label":"Consumption","pattern":None,"type":"number"},
        {"id":"","label":"Mortality","pattern":None,"type":"number"},
        {"id":"","label":"Healthcare","pattern":None,"type":"number"},
        {"id":"","label":"Life expectancy","pattern":None,"type":"number"},
        {"id":"","label":"Broadband","pattern":None,"type":"number"},
        {"id":"","label":"Per capita","pattern":None,"type":"number"}
        ]


lines = []

for col in cols:
    line = "data.addColumn('%s', '%s');" % (col['type'],col['label'])
    lines.append(line)

lines.append("data.addRows([")

with open('/home/harry/downloads/data/alldata.csv', mode='r') as infile:
    reader = csv.reader(infile, delimiter='\t')
    odata = []
    rows = []
    for i, row in enumerate(reader):
        if i == 0: continue
        if i > LIMIT: break
        consumption_vals = [1,3,4,7,10,13,16,19,22,25,28]
        electricity_vals = map(lambda x: x+1, consumption_vals)
        mortality_vals = map(lambda x: x+1, electricity_vals)
        years = range(2000,2011)

        country = row[0]

        for year in years:
            consumption = str(convert_float(row[(year-2000)*7+1]))
            electricity = str(convert_float(row[(year-2000)*7+2]))
            mortality = str(convert_float(row[(year-2000)*7+3]))
            healthcare = str(convert_float(row[(year-2000)*7+4]))
            life_expectancy = str(convert_float(row[(year-2000)*7+5]))
            broadband = str(convert_float(row[(year-2000)*7+6]))
            per_capita = str(convert_float(row[(year-2000)*7+7]))
            if all(map(lambda x: len(x)>0 and x!="0", [consumption, electricity, mortality, healthcare, life_expectancy, broadband, per_capita])):
                line = "[\"%s\",  new Date (%d,0,1), %s, %s, %s, %s, %s, %s, %s]," % (country, year, electricity, consumption, mortality, healthcare, life_expectancy, broadband, per_capita)
                lines.append(line)

lines.append("]);")
lines = "\n".join(lines)
open("/tmp/fucktards.js","wb").write(lines)
