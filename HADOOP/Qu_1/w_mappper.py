#!/usr/bin/env python
import sys

for line in sys.stdin:
	line=line.strip()
	
	year = line[15:19]
	print ('%s\t%s' % (year, "1"))
