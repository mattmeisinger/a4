#!/usr/bin/env python
import boto
import boto.emr
from boto.emr.step import StreamingStep
#from boto.emr.bootstrap_action import BootstrapAction
import time

# set your aws keys and S3 bucket, e.g. from environment or .boto
S3_BUCKET='enron-matt'
NUM_INSTANCES=1

folders = ['s3n://mmeisinger-storage/enron-input/allen-p/straw',
           's3n://mmeisinger-storage/enron-input/allen-p/contacts']

conn = boto.connect_emr()
#bootstrap_step = BootstrapAction("download.tst", "s3://elasticmapreduce/bootstrap-actions/download.sh", None)
step1 = StreamingStep(
  name='Get messages (key is message id)',
  mapper='s3n://' + S3_BUCKET + '/py/messages-mapper.py',
  #mapper='s3n://elasticmapreduce/samples/wordcount/wordSplitter.py',
  #cache_files = ["s3n://" + S3_BUCKET + "/boto.mod#boto.mod"],
  reducer='aggregate',
  input=folders,
  output='s3n://' + S3_BUCKET + '/output/enron-messages')
 
jobid = conn.run_jobflow(
    name="Wordcount Enron Custom",
    log_uri="s3://" + S3_BUCKET + "/logs",
    steps = [step1],
    #bootstrap_actions=[bootstrap_step],
    num_instances=NUM_INSTANCES)

state = conn.describe_jobflow(jobid).state
print "job state = ", state
print "job id = ", jobid
# while state != u'COMPLETED':
#     print time.localtime()
#     time.sleep(30)
#     state = conn.describe_jobflow(jobid).state
#     print "job state = ", state
#     print "job id = ", jobid
 
# print "final output can be found in s3://" + S3_BUCKET + "/output" + TIMESTAMP
# print "try: $ s3cmd sync s3://" + S3_BUCKET + "/output" + TIMESTAMP + " ."

