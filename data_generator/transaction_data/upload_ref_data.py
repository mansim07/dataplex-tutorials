import math
from operator import ne
import faker
from faker import Faker
import random
import numpy as np
import sys
import datetime
from datetime import date
from datetime import timedelta
import fileinput
import random
from collections import defaultdict
import json
#import demographics
#from main_config import MainConfig
from datetime import timezone, datetime
import csv
#from faker_credit_score import CreditScore
import os
import glob
from csv import reader
import time
from google.cloud import storage

project_id= sys.argv[1]
bucket_name= sys.argv[2]
trans_ref_data=sys.argv[3]

client = storage.Client(project=project_id)
bucket = client.get_bucket(bucket_name)
#transaction_blob = bucket.blob("ref_data/")0

files=os.listdir(trans_ref_data)
print(files)
for f in files:
    print(trans_ref_data +"/{0}".format(f))


def upload_pyspark_file(filename, file):
    # """Uploads the PySpark file in this directory to the configured
    # input bucket."""
    # print('Uploading pyspark file to GCS')
    # client = storage.Client(project=project_id)
    # bucket = client.get_bucket(bucket_name)
    print('Uploading from ', file, 'to', filename)
    blob = bucket.blob("ref_data/" + filename.split(".")[0] + "/" + filename)
    blob.upload_from_filename(file)

for f in files: 
    upload_pyspark_file(f, trans_ref_data +"/{0}".format(f))

