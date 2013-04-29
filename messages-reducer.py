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
    # only process lines that include a tab
    if line.find('\t') > -1:

    	# get key and value from line, from first tab
	    key, val = line.split('\t', 1)

	    # only return key if it isn't a duplicate of the one before it
	    if key == current_key:
	    	continue
	    else:
	    	current_key = key
	    	print '%s\t%s' % (key, val)