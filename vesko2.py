from pymongo import MongoClient
import csv
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
years = range(2000,2011)

def convert_float(string):
    if string == '': return 0
    else:
       string = string.replace(",","")
       try:
           return float(string)
       except:
           return 0
data = coll.find()
countries = ["United Kingdom", "Russia", "Panama", "Moldova", "Luxembowrg", "Lebanon", "Iceland", "Ethiopia", "Haiti", "Czech Republic", "Cambodia"]
data = filter(lambda x: x['country'] in countries, data)
for i in range(len(data)):
    del data[i]['_id']

keys = ['country', 'country_code']
for year in years:
    keys.append("broadband %d" % year)
    keys.append("population %d" % year)
    keys.append("life %d" % year)
    keys.append("child %d" % year)
    keys.append("health %d" % year)
    keys.append("electricity %d" % year)
    keys.append("out_of_school %d" % year)
    keys.append("roads %d" % year)
    keys.append("gdp %d" % year)
    keys.append("unemployment %d" % year)
    keys.append("eiti_gov %d" % year)
    keys.append("eiti_company %d" % year)
    keys.append("coal %d" % year)
    keys.append("oil %d" % year)
    keys.append("gas %d" % year)
    keys.append("btu_total_production %d" % year)
    keys.append("us_total_production %d" % year)
    keys.append("consumption %d" % year)


f = open('data_vesko.csv', 'wb')
f.write(u'\ufeff'.encode('utf8'))
dict_writer = csv.DictWriter(f, keys)
dict_writer.writer.writerow(keys)
dict_writer.writerows(data)
