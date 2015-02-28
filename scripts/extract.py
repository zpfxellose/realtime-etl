import os
import os.path
import time
from time import strftime
from os import listdir
import xml.etree.ElementTree as ET
import subprocess
from setup import *
from itertools import groupby
from operator import itemgetter

dataFolder = curFolder + "/data/extract"
logFolder = curFolder + "/log/extract"
logFilePath = logFolder + "/" + strftime("%Y-%m-%d", time.localtime(time.time())) + ".log"

if not os.path.isdir(logFolder):
    os.makedirs(logFolder)

logFile = open(logFilePath, 'a')

for src in sourceSet:

    #print src
    targetDir = dataFolder + "/" + src[0].host + "_" + src[1] + "/" +  src[2] + strftime("/%Y_%m/", time.localtime(time.time())) + strftime("%d/", time.localtime(time.time()))
    targetFile = targetDir + strftime("%H_%M_%S", time.localtime(time.time())) + ".data"
    t =  getLatestTime(src[0].host, src[1], src[2], dataFolder)

    print " ".join(t) + " " + targetFile
#    printAndLog(targetFile, logFile)
    if t[2] == '99_99_99':
        cmd = [programFolder + "/extract.sh", "full", targetFile, src[0].name, src[1], src[2], src[3]]
	
#	print " ".join(cmd)
        for l in run_command(cmd):
#	    print l
            printAndLog(l, logFile)
    else:
        latestFile = dataFolder + "/" + src[0].host + "_" + src[1] + "/" + src[2] + "/" + t[0] + "/" + t[1] + "/" + t[2] + ".data"
        #print targetFile
        #print latestFile
        cmd = [programFolder + "/extract.sh", "inc", targetFile, latestFile, src[0].name, src[1], src[2], src[3]]
        print " ".join(cmd)
        for l in run_command(cmd):
            printAndLog(l, logFile)

logFile.close()
