import subprocess
import os
import sys

def PrintHelp():
    print '\t INCORRECT USAGE'
    print '\t'
    print '\t USAGE: <filename.pl> <hdfs-location>'
    exit(1)
    
    
def HdfsLs(dir):
    # TODO: Make it OS aware
    hdfsCmd=os.path.join(os.environ['HADOOP_HOME'], 'bin', 'hdfs.cmd')
    
    # Execute command
    # TODO: limit it to only directories
    opBytes = subprocess.check_output([hdfsCmd, 'dfs', '-ls', dir])
    out_text = opBytes.decode('utf-8')
    if len(out_text) == 0:
        raise Exception("hdfs dfs -ls command failed")
        
    opLines=out_text.split(os.linesep)

    paths = []
    for line in opLines:
      parts = line.split(None, -1)
      if len(parts) > 0:
        lpart = parts[-1]
        
        # Assumption: Paths comes as listed and are always lower-cased
        if lpart.startswith(dir):
            paths.append(line.split(None, -1)[-1])
            
    return paths
    


argLen = len(sys.argv)
if (argLen <= 1):
    PrintHelp()

hdfsCmd=os.path.join(os.environ['HADOOP_HOME'], 'bin', 'hdfs.cmd')
hiveCmd=os.path.join(os.environ['HIVE_HOME'], 'bin', 'hive.cmd')

hdfsLoc = sys.argv[1].lower()
subdirs = HdfsLs(hdfsLoc)

for dir in subdirs:
    print dir

    




