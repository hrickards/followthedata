from pymongo import MongoClient
import scipy, scipy.stats
import numpy as np
import cherrypy
import json
import itertools

client = MongoClient()
db = client.extractives
coll = db.data
data = list(coll.find())
years = range(2000,2011)

def convert_float(string):
    if string == '': return 0
    else:
       try:
           return float(string)
       except:
           return 0


def total_extractives(datum):
    for year in years:
        oil_key = 'oil %d' % year
        gas_key = 'gas %d' % year
        coal_key = 'coal %d' % year
        datum['btu_total_production'] = (5.8*convert_float(datum[oil_key]) if oil_key in datum else 0) + (1000*convert_float(datum[gas_key]) if gas_key in datum else 0) + (27.7*convert_float(datum[coal_key]) if coal_key in datum else 0)
    return datum

INTERESTING_KEYS = ["broadband", "gdp"]
INTERESTING_KEYS.append("btu_total_production")
matrices = []

# data = map(total_extractives, data)
for key in INTERESTING_KEYS:
    m = []
    for year in years:
        key_key = '%s %d' % (key, year)
        m.append(map(lambda r: (convert_float(r[key_key]) if key_key in r else 0), data))

    m = np.matrix(m)
    matrices.append(m)


for i in range(2):
    m = matrices[i]
    n = []
    for j in range(np.shape(m)[1]):
        col1 = m[j:,0]
        if any(map(lambda x: x != 0, col1)):
            col2 = matrices[2][j:,0]
            n.append(scipy.stats.pearsonr(col1, col2))
    n = np.array(n)

# Correlation between columns of matrices[0,1] and matrices[2]
