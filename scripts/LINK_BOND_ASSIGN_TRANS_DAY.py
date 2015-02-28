import sys
from setup import *

#arg1: input file
#arg2: output file

inFile = sys.argv[1]
outFile = sys.argv[2]

host = ""
schema = ""
table = ""
dataFolder = ""

pathList = outFile.split('/')

length = len(pathList)
i = 0
while i < length:
    if pathList[i] == "data":
        j = 0
        while j <= i:
            dataFolder = dataFolder + "/" + pathList[j]
            j = j+1
        dataFolder = dataFolder + "/" + pathList[i+1]
        hostschema = pathList[i+2]
        table = pathList[i+3]
        break
    i = i + 1

dataFolder = dataFolder[1:]
host = hostschema.split('_')[0]
schema = hostschema.split('_')[1]
lastTime = getLatestTime(host,schema,table,dataFolder)

outFolder = "/".join(outFile.split('/')[0:-1])
if not os.path.isdir(outFolder):
    os.makedirs(outFolder)

with open(inFile,'r') as inF:
    with open(outFile, 'a') as outF:
        for line in inF:
            cols = line.split('\t')
            if cols[1] == "NULL":
                continue
            # print ' '.join(cols)
            vals = cols[1].split('|')[3:]

            for v in vals:
                outF.write('\t'.join(list([cols[0],v,cols[2]])))
        outF.close
    inF.close
