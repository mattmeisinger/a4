#!/usr/bin/env python

import sys

msg_id = None
msg_from = None
msg_to = []

in_field_to = False
in_field_cc = False

# input comes from STDIN (standard input)
for line in sys.stdin:

	# remove leading and trailing whitespace
	line = line.strip()

	# Don't continue if an empty line is found (this means we must be in the body of the email)
	if not line:
		break

	# reset flag if this line is declaring a new field
	if line.find(':') > -1:
		in_field_to = False
		in_field_cc = False

	# Check if the line is the message id
	if line.startswith('Message-ID: '):
		msg_id = line.replace('Message-ID: ','')

	# Check if the line is the from line
	if line.startswith('From: ') and not msg_from:
		msg_from = line.replace('From: ','')

	if line.startswith('To: ') or in_field_to:
		msg_to = msg_to + [a.strip() for a in line.replace('To: ','').split(',') if a.find('@') > -1]
		in_field_to = True

	if line.startswith('Cc: ') or in_field_cc:
		msg_to = msg_to + [a.strip() for a in line.replace('Cc: ','').split(',') if a.find('@') > -1]
		in_field_cc = True

# Check to see if there is a valid from user and message ID.  Only output data if there is.
if msg_id and msg_from and msg_to:
	print msg_id + '\t' + msg_from + '|' + ','.join(msg_to)