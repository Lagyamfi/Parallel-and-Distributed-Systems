#!/usr/bin/env python
import sys

year2count= {}

for line in sys.stdin:
	line=line.strip()
	
	year,count = line.split('\t',1)

	try:
		count = int(count)
	except ValueError:
		continue
	
	try:
		year2count[year] = year2count[year]+count
	except:
		year2count[year] = count

for year in year2count.keys():
	print ('%s\t%s' % ( year, year2count[year]))




