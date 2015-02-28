import os
import os.path
import time
from time import strftime
from os import listdir
import xml.etree.ElementTree as ET
import subprocess
from setup import *

dataFolder = curFolder + "/data/transform"
logFolder = curFolder + "/log/transform"
logFilePath = logFolder + "/" + strftime("%Y-%m-%d", time.localtime(time.time())) + ".log"

if not os.path.isdir(logFolder):
    os.makedirs(logFolder)

logFile = open(logFilePath, 'a')

def getNewExtractPath(year_mon,day,time,src):
    tableDir = curFolder + "/data/extract/" + src.db.host + "_" + src.schema + "/" + src.table
    
    paths = []
    for yDir in listdir(tableDir):
        if yDir == year_mon:
            for dDir in listdir(tableDir + '/' + yDir):
                if dDir == day:
                    for tDir in listdir(tableDir + '/' + yDir + '/' + dDir):
                        if tDir.split(".")[0] > time:
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
    #printAndLog(conf.target.table, logFile)
#    print colIndexes

    t =  getLatestTime(conf.target.db.host, conf.target.schema, conf.target.table, dataFolder)
    targetDir = dataFolder + "/" + conf.target.db.host + "_" + conf.target.schema + "/" +  conf.target.table + strftime("/%Y_%m/", time.localtime(time.time())) + strftime("%d/", time.localtime(time.time()))
    if not os.path.exists(targetDir):
        os.makedirs(targetDir)
    targetFile = targetDir + strftime("%H_%M_%S", time.localtime(time.time())) + ".data"

    if conf.target.table == "LINK_BOND_ADD_ISSUE":
        continue

    for src in conf.sources:
        cmd = ['mysql','--login-path=' + src.db.name, '--skip-column-names','--raw', '-e', 'desc ' + src.schema + '.' + src.table]
        colNames = list(l.strip().split('\t')[0].strip() for l in run_command(cmd))
        colIndexes = list(colNames.index(m) for m in conf.mappingOrders)

        srcPaths = getNewExtractPath(t[0],t[1],t[2], src)

        if srcPaths == []:
            continue
    
        srcPaths.sort()

        cnt = 0
        with open(targetFile, 'a') as tarF:
#        printAndLog("Source File:")
            for path in srcPaths:
            #printAndLog(path, logFile)
                with open(path, 'r') as f:
                    for line in f:
#		    printAndLog(line,logFile)
		        valid = 1
                        cols = line.split('\t')

		    # filtering
      		        for aFilter in src.filters:
#			printAndLog(aFilter.colname,logFile)
#			printAndLog(str(aFilter.values), logFile)
			    fIndex = colNames.index(aFilter.colname)
			    if cols[fIndex] not in aFilter.values:
			        valid = 0
		        if valid == 1:
                            resLine = '\t'.join(list(cols[index].strip() for index in colIndexes))
			#printAndLog(resLine,logFile)
                            cnt = cnt + 1
                            tarF.write(resLine + '\n')
                    f.close()
            tarF.close()

        if cnt == 0:
	    os.remove(targetFile)
        else:
	    printAndLog(targetFile + ": " + str(cnt) + " New Lines", logFile)

    if conf.distinct == True and os.path.isfile(targetFile):
        cmdDistinct = ['sort', '-u', '-o', targetFile, targetFile]
        for l in run_command(cmdDistinct):
            printAndLog(l, logFile)

logFile.close()
