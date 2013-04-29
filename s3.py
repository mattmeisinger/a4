import boto.s3
from boto.s3.key import Key


def delete_folder(bucket_name, folder_name):
	""" Deletes an S3 folder and all of its contents, recursively """
	s3 = boto.connect_s3()
	bucket = s3.get_bucket(bucket_name)
	bucketListResultSet = bucket.list(prefix=folder_name)
	result = bucket.delete_keys([key.name for key in bucketListResultSet])
	return result


def upload_to_key(bucket_name, file_name, key_name):
	""" Upload a file to an S3 bucket (overwrites any existing file) """
	conn = boto.connect_s3()
	bucket = conn.get_bucket(bucket_name)
	key = bucket.get_key(key_name)
	if key != None:
		key.delete()
	key = Key(bucket)
	key.key = key_name
	key.set_contents_from_file(file(file_name))
	key.make_public()


def get_folders(bucket_name, folder_name):
	""" Get a list of all S3 folders with files in them under the specified folder """
	s3 = boto.connect_s3()
	bucket = s3.get_bucket(bucket_name)

	# Get all files that start with the folder name
	bucketListResultSet = bucket.list(prefix=folder_name)
	keys = [key.name for key in bucketListResultSet]

	# Skip the first key, because this key is always just the root path (which doesn't have any files in it)
	keys = keys[1:]

	# Get the reslts
	results = []
	for k in keys:
		# Trim off the actual filename and just process the path
		prefix = k[:k.rindex('/')]
		res = 's3n://' + bucket_name + '/' + prefix
		# Only add the path to the results if it doesn't already exist in the list
		if not res in results:
			results.append(res)

	return results