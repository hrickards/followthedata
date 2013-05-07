from pymongo import MongoClient
import sys
import csv
client = MongoClient()
db = client.extractives

broadband = db.broadband
population = db.population
life = db.life_expectancy
child = db.child_mortality
health = db.health_expenditure
electricity = db.electricity
out_of_school = db.out_of_school
roads = db.roads
gdp = db.gdp
unemployment = db.unemployment
eiti = db.eiti
oil = db.oil
consumption = db.consumption
gas = db.gas
coal = db.coal
coll = db.data
prices = db.prices

with open('mappings.csv', mode='r') as infile:
    reader = csv.reader(infile)
    mappings = {rows[0]:rows[1] for rows in reader}

with open('country_codes.csv', mode='r') as infile:
    reader = csv.reader(infile)
    codes = {rows[0]:rows[1] for rows in reader}

def convert_float(string):
    if string == '': return float('nan')
    else:
       try:
           return float(string)
       except:
           return float('nan')

years = range(2000,2011)

def fix_country(country):
    if country not in mappings: return country
    ncountry = mappings[country]
    if ncountry == "REMOVE": return ''
    else: return ncountry

odata = []
nndata = []
# countries = set(map(lambda x: x['Country'], broadband.find({},{'Country':1})) + map(lambda x: x['Country'], life.find({},{'Country':1})) + map(lambda x: x['Country'], child.find({},{'Country':1})) + map(lambda x: x['Country'], health.find({},{'Country':1})) + map(lambda x: x['Country'], electricity.find({},{'Country':1})) + map(lambda x: x['Country'], population.find({},{'Country':1})) +  map(lambda x: x['Country'], health.find({},{'Country':1})) + map(lambda x: x['Country'], eiti.find({},{'Country':1})))
countries = set(map(lambda x: x['Country'], oil.find({},{'Country':1})) + map(lambda x: x['Country'], gas.find({},{'Country':1})) + map(lambda x: x['Country'], coal.find({},{'Country':1})))

real_countries = set(filter(lambda x: len(x)>0, map(fix_country, countries)))

for country in real_countries:
    lifer = life.find_one({'Country':country})
    childr = child.find_one({'Country':country})
    healthr = health.find_one({'Country':country})
    broadbandr = broadband.find_one({'Country':country})
    populationr = population.find_one({'Country':country})
    electricityr = electricity.find_one({'Country':country})
    out_of_schoolr = out_of_school.find_one({'Country':country})
    roadsr = roads.find_one({'Country':country})
    gdpr = gdp.find_one({'Country':country})
    unemploymentr = unemployment.find_one({'Country':country})

    all_my_countries = filter(lambda x: fix_country(x) == country, countries)

    country_query = {'$in': all_my_countries}
    oilr = list(oil.find({'Country':country_query}))
    consumptionr = list(consumption.find({'Country':country_query}))
    gasr = list(gas.find({'Country':country_query}))
    coalr = list(coal.find({'Country':country_query}))

    cdata = {}
    c2data = {}
    cdata['country'] = country
    cdata['country_code'] = codes[country]
    old_data = ['life','child','health','electricity','out_of_school','roads','gdp','unemployment','broadband','population','eiti_gov','eiti_company', 'oil', 'gas', 'coal', 'btu_total_production', 'us_total_production']
    old_data = dict(zip(old_data, ['']*len(old_data)))
    for year in years:
        data = {}
        if lifer != None and len(str(lifer[str(year)]))>0: data['life'] = str(lifer[str(year)])
        else: data['life'] = old_data['life']
        if childr != None and len(str(childr[str(year)]))>0: data['child'] = str(childr[str(year)])
        else: data['child'] = old_data['child']
        if healthr != None and len(str(healthr[str(year)]))>0: data['health'] = str(healthr[str(year)])
        else: data['health'] = old_data['health']
        if electricityr != None and len(str(electricityr[str(year)]))>0: data['electricity'] = str(electricityr[str(year)])
        else: data['electricity'] = old_data['electricity']
        if out_of_schoolr != None and len(str(out_of_schoolr[str(year)]))>0: data['out_of_school'] = str(out_of_schoolr[str(year)])
        else: data['out_of_school'] = old_data['out_of_school']
        if roadsr != None and len(str(roadsr[str(year)]))>0: data['roads'] = str(roadsr[str(year)])
        else: data['roads'] = old_data['roads']
        if gdpr != None and len(str(gdpr[str(year)]))>0: data['gdp'] = str(gdpr[str(year)])
        else: data['gdp'] = old_data['gdp']
        if unemploymentr != None and year < 2008 and len(str(unemploymentr[str(year)]))>0: data['unemployment'] = str(unemploymentr[str(year)])
        else: data['unemployment'] = old_data['unemployment']
        if broadbandr != None and len(str(broadbandr[str(year)]))>0: data['broadband'] = str(broadbandr[str(year)])
        else: data['broadband'] = old_data['broadband']
        if populationr != None and len(str(populationr[str(year)]))>0: data['population'] = str(populationr[str(year)])
        else: data['population'] = old_data['population']

        for key, value in data.iteritems():
            c2data["%s %d" % (key, year)] = value

        eitir = eiti.find_one({'Country':country,'Year':year})
        if eitir != None: data['eiti_gov'] = str(eitir["Gov"])
        else: data['eiti_gov'] = old_data['eiti_gov']
        if eitir != None: data['eiti_company'] = str(eitir["Company"])
        else: data['eiti_company'] = old_data['eiti_company']

        if len(oilr) > 0: data['oil'] = str(sum(map(lambda x: convert_float(x[str(year)]), oilr)))
        else: data['oil'] = old_data['oil']
        if len(gasr) > 0: data['gas'] = str(sum(map(lambda x: convert_float(x[str(year)]), gasr)))
        else: data['gas'] = old_data['gas']
        if len(coalr) > 0: data['coal'] = str(sum(map(lambda x: convert_float(x[str(year)]), coalr)))
        else: data['coal'] = old_data['coal']
        if len(consumptionr) > 0: data['consumption'] = str(sum(map(lambda x: convert_float(x[str(year)]), consumptionr)))
        else: data['consumption'] = old_data['consumption']

        data['btu_total_production'] = str((365.25*5.8*convert_float(data['oil']) if 'oil' in data else 0) + (1000*convert_float(data['gas']) if 'gas' in data else 0) + (27.7*convert_float(data['coal']) if 'coal' in data else 0))

        pricesr = prices.find_one({'Year':year})
        data['us_total_production'] = str((365.25*pricesr['Oil']*convert_float(data['oil']) if 'oil' in data else 0) + (pricesr['Coal']*convert_float(data['coal']) if 'coal' in data else 0) + (pricesr['Gas']*convert_float(data['gas']) if 'gas' in data else 0))

        for key, value in data.iteritems():
            cdata["%s %d" % (key, year)] = value

        for key, value in data.iteritems():
            if len(value)>0: old_data[key] = value

    cdata = {k:('' if v == 'nan' or v == '--' else v.lstrip('$').lstrip('-')).encode('utf8') for k,v in cdata.items()}
    odata.append(cdata)


fdata = []

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



f = open('data.csv', 'wb')
f.write(u'\ufeff'.encode('utf8'))
dict_writer = csv.DictWriter(f, keys)
dict_writer.writer.writerow(keys)
dict_writer.writerows(odata)

coll.drop()
coll.insert(odata)
