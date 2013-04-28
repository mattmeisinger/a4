#!/usr/bin/env python

# Just de-duplicates keys

#from operator import itemgetter
import sys

current_key = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, val = line.split('\t', 1)

    if key == current_key:
    	continue
    else:
    	current_key = key
    	print '%s\t%s' % (key, value)