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
            if cols[2] == "NULL" or cols[2].strip() == '':
                continue
            #print "'" + cols[2] + "'"
            vals = cols[3].split('|')
            cols[2] = vals[0]

            if "-" in vals[1]:
                cols[3] = vals[1].split('-')[0]
                cols[4] = vals[1].split('-')[1]
            else:
                cols[3] = vals[1]
                cols[4] = vals[1]

            if "-" in vals[2]:
                cols[5] = vals[2].split('-')[0]
                cols[6] = vals[2].split('-')[1]
            else:
                cols[5] = vals[2]
                cols[6] = vals[2]

            outF.write('\t'.join(cols))
        outF.close
    inF.close
