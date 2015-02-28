import os
import os.path
import time
from time import strftime
from os import listdir
import xml.etree.ElementTree as ET
import subprocess
from setup import *
import sys

dataFolder = curFolder + "/data/load"
logFolder = curFolder + "/log/load"

logFilePath = logFolder + "/" + strftime("%Y-%m-%d", time.localtime(time.time())) + ".log"

if not os.path.isdir(logFolder):
    os.makedirs(logFolder)

logFile = open(logFilePath, 'a')


def getNewTransPath(year_mon,day,time,src):
    tableDir = curFolder + "/data/transform/" + src.db.host + "_" + src.schema + "/" + src.table
    
    paths = []
    for yDir in listdir(tableDir):
        if yDir == year_mon:
            for dDir in listdir(tableDir + '/' + yDir):
                if dDir == day:
                    for tDir in listdir(tableDir + '/' + yDir + '/' + dDir):
                        if tDir[:len(tDir)-5] > time:
                            paths.append(tableDir + '/' + yDir + '/' + dDir + '/' + tDir)
                elif dDir > day:
                    for tDir in listdir(tableDir + '/' + yDir + '/' + dDir):
                        paths.append(tableDir + '/' + yDir + '/' + dDir + '/' + tDir)
        elif yDir > year_mon:
            for dDir in listdir(tableDir + '/' + yDir):
                for tDir in listdir(tableDir + '/' + yDir + '/' + dDir):
                     paths.append(tableDir + '/' + yDir + '/' + dDir + '/' + tDir)

    return paths


for conf in confs:
    cmd = ['mysql','--login-path=' + conf.target.db.name, '--skip-column-names','--raw', '-e', 'desc ' + conf.target.schema + '.' + conf.target.table]
    tarColNamesAll = list(l.strip().split('\t')[0].strip() for l in run_command(cmd))

    colNum = len(conf.mappingOrders)
    tarColNames = tarColNamesAll[1:colNum + 1]
#    print tarColNames

    t =  getLatestTime(conf.target.db.host, conf.target.schema, conf.target.table, dataFolder) 
    srcPaths = getNewTransPath(t[0],t[1],t[2], conf.target)

    if srcPaths == []:
	continue

    srcPaths.sort()

#    for path in srcPaths:
#	print path

    targetDir = dataFolder + "/" + conf.target.db.host + "_" + conf.target.schema + "/" +  conf.target.table + strftime("/%Y_%m/", time.localtime(time.time())) + strftime("%d/", time.localtime(time.time()))
    if not os.path.exists(targetDir):
        os.makedirs(targetDir)
    targetFile = targetDir + strftime("%H_%M_%S", time.localtime(time.time())) + ".load"

    wFile = open(targetFile, 'w')
    wFile.write("start transaction;\n");
    for path in srcPaths:
#        print(path)
        with open(path, 'r') as fr:
            cnt = 0
            for line in fr:
		cmdbody = "insert into " + conf.target.schema + "." + conf.target.table + " (" + ','.join(tarColNames) + ", DATA_INSERT_TIME) values ('" +  line.replace("\n","").replace("'","\\\'").replace("\t","\',\'") + "\', now()) on duplicate key update " + reduce(lambda x,y: x + "," + y, map(lambda x: x + "=Values(" + x +")", tarColNames)) + ", DATA_UPDATE_TIME = now()"

#                print cmdbody
		wFile.write(cmdbody + ";\n")

		#print " ".join(cmd2 + [cmdbody])
		
	#	for l in run_command(cmd2 + [cmdbody]):
	#	    printAndLog(' '.join(cmd2 + [cmdbody]))
	#	    printAndLog(l)
	#	    sys.exit(1)
		cnt = cnt + 1
            fr.close()
	printAndLog(path + ": " + str(cnt) + " Record Generated", logFile)
    wFile.write("commit;\n");
    wFile.close()
    cmd3 = ['./load.sh',targetFile, conf.target.db.name]
#    print cmd3
    hasError = 0
    for l in run_command(cmd3):
        printAndLog(l, logFile)
        hasError = 1
    
    if hasError == 0:
        with open(targetFile + "ok", 'w') as f:
            f.close()
        os.remove(targetFile)

logFile.close()
