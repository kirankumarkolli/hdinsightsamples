import subprocess
import os
import sys
import re
import ntpath

# TODO: Make it OS aware
hdfsCmd=os.path.join(os.environ['HADOOP_HOME'], 'bin', 'hdfs.cmd')
hiveCmd=os.path.join(os.environ['HIVE_HOME'], 'bin', 'hive.cmd')

def PrintHelp():
    print '\t INCORRECT USAGE'
    print '\t'
    print '\t USAGE: <filename.pl> <hdfs-location>'
    exit(1)
    
    
def HdfsLs(dir):
    # Execute command
    # TODO: limit it to only directories
    opBytes = subprocess.check_output([hdfsCmd, 'dfs', '-ls', dir])
    out_text = opBytes.decode('utf-8')

    paths = []
    for line in out_text.split(os.linesep):
      parts = line.split(None, -1)
      if len(parts) > 0:
        lpart = parts[-1]
        
        # Assumption: Paths comes as listed and are always lower-cased
        if lpart.startswith(dir):
            paths.append(line.split(None, -1)[-1])
            
    return paths
    
def HdfsTableDef(tablename):
    # c:\apps\dist\hive-0.14.0.2.2.6.1-0012\bin\hive -e "DESCRIBE EXTENDED hivesampletable"
    descTblCmd = "DESCRIBE EXTENDED {0}".format(tablename)
    opBytes = subprocess.check_output([hiveCmd, '-e', descTblCmd])
    out_text = opBytes.decode('utf-8')
    
    locationRe = re.compile('(.*, )location:(.*), input(.*)')
    partKeysRe = re.compile('(.*, )partitionKeys:\[(.*)\](.*)')
    fieldSchemaRe = re.compile('(.*)FieldSchema\(name:(.*), type(.*)');
    
    result = []
    for line in out_text.split('\n'):
        match = locationRe.match(line)
        if match:
            result.append(match.group(2))

        match = partKeysRe.match(line)
        if match:
            for fieldSchemaEntry in match.group(2).split("),"):
                match = fieldSchemaRe.match(fieldSchemaEntry)
                if match:
                    result.append(match.group(2))
            
    return result


def GetPart(location, partprefix, parts):
    if len(parts) == 0:
        return [partprefix, location];
    
    part = parts[0]
    result = []
    paths = HdfsLs(location)
    for path in paths:
        newPartPrefix = ", ".join([partprefix, "{0}={1}".format(part, ntpath.basename(path))])
        result = result + GetPart(path, newPartPrefix, parts[1:])
    
    return result;
    
        
argLen = len(sys.argv)
if (argLen <= 1):
    PrintHelp()

tablename = sys.argv[1].lower()
tblDef = HdfsTableDef(tablename)
#print (tblDef)



tblLocation = tblDef[0]
result = GetPart(tblLocation, "", tblDef[1:])
l=len(result)
s=0

while s < l:
    p = result[s][2:]
    hl = result[s+1]
    
    print ("ALTER TABLE {0} ADD PARTITION ({1}) location '{2}';".format(tablename, p, hl))
    s = s + 2
    