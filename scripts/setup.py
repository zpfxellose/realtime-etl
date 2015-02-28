import os
import os.path
import time
from time import strftime
from os import listdir
import xml.etree.ElementTree as ET
import subprocess
from itertools import groupby
from operator import itemgetter
from sets import Set

curFolder = os.getcwd() #os.path.dirname(os.path.abspath(__file__))
confFolder = curFolder + "/conf"
confFile = ET.parse(confFolder + "/conf_example.xml")
programFolder = curFolder + "/scripts"

def run_command(command):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

class DB:
    def __init__(self,nameVal, hostVal, usernameVal, passwordVal,schemaVal):
        self.name = nameVal
        self.host = hostVal
        self.username = usernameVal
        self.password = passwordVal
        self.schema = schemaVal

class SourceTable:
    def __init__(self, dbVal, schemaVal, tableVal, frequencyVal, mappingOrdersVal, orderByVal):
        self.db = dbVal
        self.schema = schemaVal
        self.table = tableVal
        self.frequency = frequencyVal
	self.filters = filterVal
	self.mappingOrders = mappingOrdersVal
	self.orderBy = orderByVal

class Table:
    def __init__(self, dbVal, schemaVal, tableVal):
        self.db = dbVal
        self.schema = schemaVal
        self.table = tableVal

class Conf:
    def __init__(self, tarVal, sourcesVal, typeVal, frequencyVal, distinctVal):
        self.target = tarVal
        self.sources = sourcesVal
        self.mappingType = typeVal
        self.frequency = frequencyVal
	self.distinct = distinctVal

class Filter:
    def __init__(self, colVal, valuesVal):
	self.colname = colVal
	self.values = valuesVal

DBs = [DB(d.find('name').text, d.find('host').text, d.find('username').text, d.find('password').text, d.find('schema').text) for d in confFile.find('databases').iter('database')]


# def getSource(srcXml):
#     db = next(d for d in DBs if d.name == srcXml.find("database").text)
#     filters = []
#     if srcXml.find('filters') is not None:
#         for f in srcXml.find('filters').iter('filter'):
# 	    filters.append(Filter(f.find('column').text,list(v.text for v in f.find('values').findall('value'))))
#     mappingOrders = list(sc.text for sc in srcXml.find('mappingOrder').findall('sourceCol'))
#     orderBy = srcXml.find('orderBy').text
#     return SourceTable(db, srcXml.find('schema').text, srcXml.find('table').text, srcXml.find('frequency').text,filters, mappingOrders, orderBy)

# confs = []
# for c in confFile.find('configures').iter('configure'):
#     if c.find('target') is None:
#         continue

#     tardb = next(d for d in DBs if d.name == c.find('target').find('database').text)
#     target = Table(tardb, c.find('target').find('schema').text, c.find('target').find('table').text)
    
#     sourceTs = []
#     for s in c.iter('source'):
#         sourceTs.append(getSource(s))

#     mappingType = c.find('type').text
#     frequency = c.find('frequency').text

#     distinct = False
#     if c.find('distinct') is None:
# 	distinct = False
#     elif c.find('distinct').text == 'true':
# 	distinct = True

#     confs.append(Conf(target,sourceTs, mappingType, frequency, distinct))
    
	
sourceTables = Set([])

sourcesList = []
for src in confFile.find('configures').iter('source'):
    filters = []
    if src.find('filters') is not None:
        for f in src.find('filters').iter('filter'):
            filters.append(Filter(f.find('group_name'),list(v.text for v in f.find('values').findall('value'))))

    mappingOrders = list(sc.text for sc in src.find('mappingOrder').findall('sourceCol'))
    orderBy = src.find('orderBy').text
    sourcesList.append([[src.find('database').text, src.find('schema').text, src.find('table').text, filters, mappingOrders, orderBy], src.find('frequency').text])

# for src, freqs in groupby(sourcesList, itemgetter(0)):
#     r = src + [(min(list(f[1] for f in freqs)))]
#     srcdb = next(d for d in DBs if d.name == r[0])
#     print r
#     sourceTables.add(SourceTable(srcdb,r[1],r[2],r[6],r[3], r[4], r[5]))

#sourceSet = Set((src[0][0], src[0][1], src[0][2]) for src in sourcesList)
sourceSet = Set([])

for src in sourcesList:
    srcdb = next(d for d in DBs if d.name == src[0][0])
    sourceSet.add((srcdb, src[0][1], src[0][2], src[0][5]))
#print sourceSet

del sourcesList


def printAndLog(str, logFile):
    if str is None:
	return
    if str.strip() == "" or str =="\n":
        return
    str.replace("\n"," ")
    logFile.write(strftime("%Y-%m-%d %H:%M:%S: ", time.localtime(time.time())) + str.strip() + "\n")
    #print str.strip()

def getLatestTime(host, schema, table, dataFolder):
    tableDir = dataFolder + "/" + host + "_" + schema + "/" + table
    if not os.path.isdir(tableDir):
        return ['0000_00','00','99_99_99']

    # print tableDir
    # print listdir(tableDir)
    if listdir(tableDir) == []:
        return ['0000_00','00','99_99_99']
    yyyy_mm = max(listdir(tableDir))
    tableYearDir = tableDir + "/" + yyyy_mm
    if not os.path.isdir(tableYearDir):
        return ['0000_00','00','99_99_99']

    # print listdir(tableYearDir)
    if listdir(tableYearDir) == []:
	os.rmdir(tableYearDir)
	return getLatestTime(host,schema,table,dataFolder)
    dd = max(listdir(tableYearDir))
    tableYearDayDir = tableYearDir + "/" + dd
    if not os.path.isdir(tableYearDayDir):
        return [yyyy_mm,'00','99_99_99']

    if listdir(tableYearDayDir) == []:
	os.rmdir(tableYearDayDir)
	return getLatestTime(host,schema,table,dataFolder)
    # print listdir(tableYearDayDir)
    hh_mm_ss = max(listdir(tableYearDayDir))
    if hh_mm_ss is None:
        hh_mm_ss = ['99_99_99']

    hh_mm_ss = hh_mm_ss.split('.')[0]
    return [yyyy_mm,dd,hh_mm_ss]
