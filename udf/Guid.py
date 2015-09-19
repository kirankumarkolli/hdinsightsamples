#!/usr/bin/env python
import sys
import string
import uuid

while True:
  line = sys.stdin.readline()
  if not line.strip():
    break

  name = string.strip(line, "\n ")
  
  try:
    uuid.UUID(name).hex
    print 1
  except: 
    print 0
  