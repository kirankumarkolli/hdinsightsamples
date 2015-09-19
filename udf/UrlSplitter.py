#!/usr/bin/env python
import sys
import string
import uuid

def isguid(str):
  try:
    uuid.UUID(str)
    return "1"
  except: 
    return "0"

while True:
  line = sys.stdin.readline()
  if not line.strip():
    break

  line = string.strip(line, "\n ")
  path, unitname, length = string.split(line, "\t")
  pathsplit = string.split(path, "/", int(length)+1);

  if len(pathsplit) > int(length):
      pathsplit.reverse();
      pathsplit.pop(0);
      pathsplit.reverse();
      #pathsplit.pop(0);
  
  subpath = "/".join(pathsplit)
  ig=isguid(unitname)
  print "\t".join([subpath, unitname, ig])
  
  #result = []
  #subpath = ""
  #for item in pathsplit:
  #  subpath = "/".join([subpath, item])
  #  result.append([subpath, unitname])
  #print "\t".join(result);