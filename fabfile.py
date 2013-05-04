#!/usr/bin/env python
import boto
import boto.emr
from boto.emr.step import StreamingStep
import time
import os
import s3


# Look at the test-hadoop-local.sh file first...as that is what I was working on last.


# Working S3 bucket where all intermediary files and output files will go (this
# will need to be changed to a bucket you own, as I don't know how to allow
# you access to my 'enron-matt' bucket?)
S3_BUCKET='enron-matt'

# Bucket where the publicly-accessible source files are
SOURCE_S3_BUCKET='mmeisinger-storage'
SOURCE_S3_FOLDER='enron-input'
NUM_INSTANCES=1

# Folder where the output for step one should be placed, in the S3_BUCKET
OUTPUT_STEP1_FOLDER = 'output/step1'

# Folder where the mapper and reducer JARs are stored in the S3_BUCKET
JAR_FOLDER = 'jar/'

# JAR file with the MapReduce class(es) in it
MAP_REDUCE_JAR_PATH = 'java/MapReduce/EmailGrapher.jar'

def upload_scripts():
	""" Upload Map/Reduce Java JAR to S3_BUCKET """
	s3.upload_to_key(bucket_name=S3_BUCKET,file_name=MAP_REDUCE_JAR_PATH,key_name='/'+JAR_FOLDER+messages_mapper)


def create_emr():
	""" Creates an Elastic Map Reduce job that uses a custom python mapper and reducer """
	# Delete any previously existing output, since EMR requires an empty output directory
	s3.delete_folder(S3_BUCKET, OUTPUT_STEP1_FOLDER)

	# Get a full list of the folders that the source exists in. Neither Hadoop nor EMR have 
	# the capability of specifying that a source directory should be read recursively.
	# Note: We may need to chunk the jobs even further into separate steps, as Hadoop has 
	# limits on how many input directories it can handle for each step.
	folders = s3.get_folders(SOURCE_S3_BUCKET,SOURCE_S3_FOLDER)

	conn = boto.connect_emr()

	# Create the first step, to scan all the messages and just pull out the message id, sender, and recipients (both 'To' and 'Cc' recipients)
	step1 = JarStep(
		name='STEP 1: Get messages and write line for each Inlink and Outlink to output files',
		jar='s3n://' + S3_BUCKET + '/' + JAR_FOLDER + messages_mapper,
		main_class=org.columbia.ExtractEmailAddresses,
		# Convention is that the first item in the string list is the input folder, and the
		# second item is the output folder
		step_args=[
			folders[0], 
			output='s3n://' + S3_BUCKET + '/' + OUTPUT_STEP1_FOLDER
		])

	# TODO: Many more step 1's...one for each input folder?

	# TODO: Create Step2 to run against: java/MapReduce/src/org/columbia/CompileInlinksOutlinks.java

	# Spin up job to run all steps
	jobid = conn.run_jobflow(
		name="Enron Mail Analysis",
		log_uri="s3://" + S3_BUCKET + "/logs",
		steps = [step1], # TODO: Add all step 1s, and one step 2 to tie them all together
		num_instances=NUM_INSTANCES)

	# Return the job id to the user.  At the point the servers will take some time to spin up and process the job.
	state = conn.describe_jobflow(jobid).state
	print "job state = ", state
	print "job id = ", jobid
