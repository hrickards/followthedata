from pymongo import MongoClient
import md5
import redis
import numpy as np
import cherrypy
import json
import itertools

client = MongoClient()
db = client.extractives
coll = db.data
coll2 = db.data2

def convert_float(string):
    if string == '': return 0
    else:
       string = string.replace(",","")
       try:
           return float(string)
       except:
           return 0
coll2.drop()
data = coll.find()
for country in data:
    res = {}
    res['country'] = country['country']
    res['per_capita_revenue'] = {}
    for year in range(2000,2011):
        pop = convert_float(country["population %d" % year])
        production = convert_float(country["us_total_production %d" % year])
        if pop != 0 and production != 0: res['per_capita_revenue'][str(year)] = production/pop
    coll2.insert(res)
