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
r = redis.StrictRedis(host='localhost', port=6379, db=0)

DEFNS = {'broadband': 'Fixed broadband Internet subscribers', 'population': 'Total population', 'life': 'Life expectancy at birth', 'child': 'Child mortality', 'health': 'Government expenditure on health per-capita', 'electricity': 'Electricity use per-capita', 'eiti_gov': 'Government Revenue (as reported by Government)', 'eiti_company': 'Government Revenue (as reported by Companies)', 'gas': 'Gross Natural Gas Production', 'oil': 'Total Oil Supply', 'coal': 'Total Primary Coal Production', 'out_of_school': 'Primary children out of school', 'roads': 'Roads paved', 'gdp': 'Income per person', 'unemployment': 'Long term unemployment', 'btu_total_production': 'Total oil, gas and coal production', 'us_total_production': 'Total oil, gas and coal production'}
UNITS = {'broadband': '%', 'population': '', 'life': ' yrs', 'child': ' 0-5 year-olds dying per 1,000 born', 'health': 'US$', 'electricity': 'kWh', 'eiti_gov': 'US$', 'eiti_company': 'US$', 'gas': ' Billion Cubic Feet', 'oil': ' Thousand Barrels Per Day', 'coal': ' Thousand Short Tons', 'out_of_school': ' children', 'roads': '%', 'gdp': 'US$', 'unemployment': '%', 'btu_total_production': 'BTU', 'us_total_production': 'US$'}

def format_data(data, field):
    try:
        return "%s: %s%s" % (DEFNS[field], "%s" % format_number(convert_float(data)), UNITS[field])
    except Exception:
        return ""

def convert_float(string):
    if string == '': return 0
    else:
       string = string.replace(",","") 
       try: return float(string)
       except: return 0

def cached_get_data(years, fields):
    hashed = md5.new(json.dumps([years,fields])).digest()
    previous = r.get(hashed)
    if previous == None:
        result = get_data(years, fields)
        r.set(hashed, result)
        return result
    else:
        return previous

def get_data(years, fields):
    fields = [fields]
    odata = {}
    for year in years:
        data = []
        country_data = list(coll.find())
        for country_datum in country_data:
            datum = {'id': country_datum['country_code']}
            for field in fields:
                datum['value'] = country_datum["%s %s"%(field,year)]
                datum['description'] = format_data(country_datum["%s %s"%(field,year)], field) + " (%s)"% str(year)
            if len(datum['value']) > 0: data.append(datum)
        odata[year] = data

    values = map(lambda data: map(lambda datum: convert_float(datum['value']), data), odata.values())
    values = [item for sublist in values for item in sublist]
    vec = np.array(values)
    sigma = np.std(vec)
    mu = np.mean(vec)

    for key in odata.keys(): odata[key] = scale_data(odata[key], mu, sigma)

    min_value = min(map(lambda data: min(map(lambda datum: datum['value'], data)), odata.values()))
    max_value = max(map(lambda data: max(map(lambda datum: datum['value'], data)), odata.values()))
    for key in odata.keys(): odata[key] = normalise_data(odata[key], min_value, max_value)
    min_value = min(map(lambda data: min(map(lambda datum: datum['value'], data)), odata.values()))
    max_value = max(map(lambda data: max(map(lambda datum: datum['value'], data)), odata.values()))
    odata['query'] = {'min': min_value, 'max':max_value}
    return odata

def old_get_data(years, fields):
    hashed = md5.new(json.dumps([years,fields])).digest()
    previous = r.get(hashed)
    if previous == None:
        result = json.dumps(get_data([str(years)], fields)[years])
        r.set(hashed, result)
        return result
    else:
        return previous

def real_get_data(fields):
    hashed = md5.new(json.dumps(fields)).digest()
    years = map(str, range(2000,2011))
    previous = r.get(hashed)
    if previous == None:
        result = json.dumps(get_data(years, fields))
        r.set(hashed, result)
        return result
    else:
        return previous

def scale_data(data, mu, sigma):
    values = map(lambda datum: convert_float(datum['value']), data)
    vec = map(lambda x: (x-mu)/sigma, np.array(values))
    new_data = []
    for i, datum in enumerate(data):
        datum['value'] = (vec[i])
        new_data.append(datum)
    return new_data

def normalise_data(data, min_value, max_value):
    values = map(lambda datum: (datum['value']), data)
    vec = map(lambda x: (x-min_value)/(max_value-min_value), np.array(values))
    new_data = []
    for i, datum in enumerate(data):
        datum['value'] = (vec[i])
        new_data.append(datum)
    return new_data


import decimal

def float_to_decimal(f):
    # http://docs.python.org/library/decimal.html#decimal-faq
    "Convert a floating point number to a Decimal with no loss of information"
    n, d = f.as_integer_ratio()
    numerator, denominator = decimal.Decimal(n), decimal.Decimal(d)
    ctx = decimal.Context(prec=60)
    result = ctx.divide(numerator, denominator)
    while ctx.flags[decimal.Inexact]:
        ctx.flags[decimal.Inexact] = False
        ctx.prec *= 2
        result = ctx.divide(numerator, denominator)
    return result 

def f(number, sigfig):
    # http://stackoverflow.com/questions/2663612/nicely-representing-a-floating-point-number-in-python/2663623#2663623
    assert(sigfig>0)
    try:
        d=decimal.Decimal(number)
    except TypeError:
        d=float_to_decimal(float(number))
    sign,digits,exponent=d.as_tuple()
    if len(digits) < sigfig:
        digits = list(digits)
        digits.extend([0] * (sigfig - len(digits)))    
    shift=d.adjusted()
    result=int(''.join(map(str,digits[:sigfig])))
    # Round the result
    if len(digits)>sigfig and digits[sigfig]>=5: result+=1
    result=list(str(result))
    # Rounding can change the length of result
    # If so, adjust shift
    shift+=len(result)-sigfig
    # reset len of result to sigfig
    result=result[:sigfig]
    if shift >= sigfig-1:
        # Tack more zeros on the end
        result+=['0']*(shift-sigfig+1)
    elif 0<=shift:
        # Place the decimal point in between digits
        result.insert(shift+1,'.')
    else:
        # Tack zeros on the front
        assert(shift<0)
        result=['0.']+['0']*(-shift-1)+result
    if sign:
        result.insert(0,'-')
    return ''.join(result)

def convert_num(number):
    if "." in number: return convert_float(number)
    else: return int(number)

def format_number(number):
    return "{:,}".format(convert_num(f(number, 4)))

class App(object):
    def index(self, year, field, callback=None):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
        return old_get_data(year, field)
    index.exposed = True

    def new(self, field, callback=None):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
        return real_get_data(field)
    new.exposed = True

    def call_all(self):
        for field in DEFNS.keys():
            print field
            real_get_data(field)
            return "Success"
    call_all.exposed = True
cherrypy.config.update({'server.socket_host': '172.16.3.8', 'server.socket_port': 9999})
cherrypy.quickstart(App())
