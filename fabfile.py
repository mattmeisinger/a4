#!/usr/bin/env python
import boto
import boto.emr
from boto.emr.step import StreamingStep
import time
import os
import s3


# NOTE: This file was created with the purpose of running streaming Python hadoop jobs.  Unfortunately
# the streaming API does not support customizing the names of the files.  So this needs to be adapted
# to deploy the Java JAR file to S3 instead of the Python scripts.

# Look at the test-hadoop-local.sh file first...as that is what I was working on last.



# Working S3 bucket where all intermediary files and output files will go
S3_BUCKET='enron-matt'

# Bucket where the publicly-accessible source files are
SOURCE_S3_BUCKET='mmeisinger-storage'
SOURCE_S3_FOLDER='enron-input'
NUM_INSTANCES=1

# Folder where the output for step one should be placed, in the S3_BUCKET
OUTPUT_STEP1_FOLDER = 'output/step1'

# Folder where the mapper and reducer python source files can be found in the S3_BUCKET
PYTHON_MAP_REDUCE_SOURCE_FOLDER = 'py/'

# Filename of the step 1 messages mapper source file
messages_mapper = 'messages-mapper.py'
messages_reducer = 'messages-reducer.py'


def upload_scripts():
	""" Upload python Mappers and Reducers to S3_BUCKET """
	s3.upload_to_key(bucket_name=S3_BUCKET,file_name=messages_mapper,key_name='/'+PYTHON_MAP_REDUCE_SOURCE_FOLDER+messages_mapper)
	s3.upload_to_key(bucket_name=S3_BUCKET,file_name=messages_reducer,key_name='/'+PYTHON_MAP_REDUCE_SOURCE_FOLDER+messages_reducer)


def create_emr():
	""" Creates an Elastic Map Reduce job that uses a custom python mapper and reducer """
	# Delete any previously existing output, since EMR requires an empty output directory
	s3.delete_folder(S3_BUCKET, OUTPUT_STEP1_FOLDER)

	# Get a full list of the folders that the source exists in. Neither Hadoop nor EMR have 
	# the capability of specifying that a source directory recursively
	folders = s3.get_folders(SOURCE_S3_BUCKET,SOURCE_S3_FOLDER)

	conn = boto.connect_emr()

	# Create the first step, to scan all the messages and just pull out the message id, sender, and recipients (both 'To' and 'Cc' recipients)
	step1 = StreamingStep(
		name='STEP 1: Get messages, and deduplicate (key is message id)',
		mapper='s3n://' + S3_BUCKET + '/' + PYTHON_MAP_REDUCE_SOURCE_FOLDER + messages_mapper,
		reducer='s3n://' + S3_BUCKET + '/' + PYTHON_MAP_REDUCE_SOURCE_FOLDER + messages_reducer,
		input=folders,
		output='s3n://' + S3_BUCKET + '/' + OUTPUT_STEP1_FOLDER)

	# Spin up job to run all steps
	jobid = conn.run_jobflow(
		name="Enron Mail Analysis",
		log_uri="s3://" + S3_BUCKET + "/logs",
		steps = [step1],
		num_instances=NUM_INSTANCES)

	# Return the job id to the user.  At the point the servers will take some time to spin up and process the job.
	state = conn.describe_jobflow(jobid).state
	print "job state = ", state
	print "job id = ", jobid
