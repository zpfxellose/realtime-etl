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

cmd = ['tail','-1',dataFolder + "/" + hostschema+ "/" + table + "/" + lastTime[0] + "/" + lastTime[1] + "/" + lastTime[2] + ".data"]

outFolder = "/".join(outFile.split('/')[0:-1])
if not os.path.isdir(outFolder):
    os.makedirs(outFolder)

###############################################################
flagCmdLookups = ['mysql','--login-path=local','--raw','--skip-column-names','-e','select * from WAREHOUSE.DICT_FLAG']
flagLookups = list(l.split('\t') for l in run_command(flagCmdLookups))

cityCmdLookups = ['mysql','--login-path=local','--raw','--skip-column-names','-e','select * from WAREHOUSE.DICT_CITY_PROVINCE']
cityLookups = list(l.split('\t') for l in run_command(cityCmdLookups))

with open(inFile,'r') as inF:
    with open(outFile, 'a') as outF:
        for line in inF:
            cols = line.split('\t')
            cols = list(c.strip() for c in cols)

            if cols[11][-1:] == '%':  # if the last char besides ' ' is %, remove them
                cols[11] = cols[11][:-1]
            elif cols[11] == 'NULL':
                cols[11] = '0'
            else:
                try:
                    float(cols[11])
                    cols[11] = cols[11]
                except ValueError:
                    print "cannot parse column issuer_info.Stockholding_Percentage\n"
                    print cols[11] + "\n"
            #city_key lookup
            found = 0
            for l in cityLookups:
                if l[2] == cols[7] and l[3] == cols[6]:
                    cols[7] = l[0]
                    found = 1
                if cols[7] == "NULL":
                    found = 1
            if found == 0:
                print "Cannot find city key " + cols[7]
                print line + "\n"
            
            #CBRC_FINANCING_PLATFORM lookup in DICT_FLAG
            found = 0
            for l in flagLookups:
                if l[2] == cols[14]:
                    cols[14] = l[0]
                    found = 1
                elif cols[14] == "NULL":
                    found = 1
            if found == 0:
                print "Cannot find CBRC_FINANCING_PLATFORM for flag_key\n"
                print line + "\n"
            outF.write('\t'.join(cols) + "\n")
        outF.close
    inF.close
