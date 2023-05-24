'''
    Schema: 
        Merchant Name 
        Merchant Address
        Merchant Phone Number 
        Merchant Category Code 
        Merchant Latitude 
        Merchant Longitude  
'''

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
from google.cloud import storage



class Merchant:
    
    def __init__(self, row, counter,mcc_code_list):
        #self.merchant_id="mer-" + ''.join(filter(str.isalnum, fake.uuid4())) 
        self.merchant_id=row[0] #fake.pystr(min_chars=14,max_chars=14)
        mcc=random.choice(mcc_code_list)
        self.merchant_name,mcc_code_list=self.get_merchant_name(mcc)
        self.mcc=mcc[0]  #random.choice(mcc[0])
        self.domain_str=''.join(filter(str.isalnum, self.merchant_name.split(' ')[0])) 
        self.domain_type = ['com', 'biz', 'net']
        self.email = fake.email(domain=self.domain_str.lower() + "." +random.choice(self.domain_type))
        self.street_address= fake.street_address()
        latlng=fake.local_latlng(country_code='US')
        self.city='' #latlng[2]
        self.state='' #latlng[4].split("/")[1]
        self.country=latlng[3]
        #location=self.get_random_location()
        self.latitude=row[2] #fake.coordinate(center=location.split('|')[3] , radius=0.1) #latlng[0]
        self.longitude=row[1] #fake.coordinate(location.split('|')[4],radius=0.1)  #latlng[1]
        self.owner_id=fake.pystr(min_chars=10,max_chars=10)
        self.owner_name=fake.name()
        self.terminal_ids="tid-" +fake.pystr_format() #,"tid-" +fake.pystr_format(),"tid-" +fake.pystr_format() 

    def get_merchant_name(self,mcc):
        
        if mcc[2] == 'Y':
            mcc_code_list.remove(mcc)
            return mcc[1],mcc_code_list
        else:
            return fake.company() + " " + mcc[1].split(' ')[0] + " " + fake.company_suffix(), mcc_code_list
def validat_parse_input():
    def print_err(n):
        if n == 1:
            print('Error: invalid number of customers')
        elif n == 2:
            print('Error: invalid (non-integer) random seed')
        elif n == 3:
            print('Error: main.config could not be opened')

        output = '\nENTER:\n (1) Number of customers\n '
        output += '(2) Random seed (int)\n '
        output += '(3) main_config.json\n'
        output += '(4) GCP project_name\n'
        output += '(5) GCS bucket_name'

        print(output)
        sys.exit(1)

    try:
        num_merchants = int(sys.argv[1])
    except:
        print_err(1)
    try:
        seed_num = int(sys.argv[2])
    except:
        print_err(2)
    try:
        merchant_output_filename = sys.argv[3]
        #merchant_output_filename = open(m, 'r').read()
    except:
        print_err(3)
    try:
        merchant_mcc_codes = sys.argv[4]
        #merchant_mcc_codes= open(m1, 'r').read()
    except:
        print_err(4)
    try:
        cc_merchant_filename = sys.argv[5]
        #merchant_mcc_codes= open(m1, 'r').read()
    except:
        print_err(4)
    try:
        project_id = sys.argv[6]
    except:
        print_err(5)

    try:
        bucket_name = sys.argv[7]
    except:
        print_err(5)
    try:
       merchant_gcs_filename = sys.argv[8]
    except:
        print_err(5)

    try:
        mcc_gcs_filename = sys.argv[9]
    except:
        print_err(5)

    return num_merchants, seed_num, merchant_output_filename, merchant_mcc_codes, cc_merchant_filename,  project_id, bucket_name, mcc_gcs_filename, merchant_gcs_filename

if __name__ == '__main__':


    num_merchants, seed_num, merchant_output_filename, merchant_mcc_codes, cc_merchant_filename, project_id, bucket_name, mcc_gcs_filename, merchant_gcs_filename = validat_parse_input()

    #merchant_source_filename=sys.argv[1]    #"./data/merchant.csv"
    #merchant_mcc_codes=sys.argv[2]
    print("generating merchant data")
    fake = Faker()
    Faker.seed(seed_num)
    merchant_per_mcc=math.ceil(num_merchants/790)

    today_date = datetime.today().strftime('%Y-%m-%d')
    ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    merchants_file = merchant_gcs_filename#"merchants_data/date=" + today_date + "/merchants_" + str(ts) + ".csv"
    mcc_file= mcc_gcs_filename#"mcc_codes/date=" + today_date + "/mcc_codes_" + str(ts) + ".csv"
    merchant_blob = bucket.blob(merchants_file)
    mcc_blob = bucket.blob(mcc_file)
    merchants_source_filename="./data/merchant.csv"
    mcc_source_filename="./data/mcc_codes.csv"


    mcc_code_list=[]
    with open(merchant_mcc_codes, 'r') as read_obj:
        csv_reader = reader(read_obj,delimiter="|")
        header = next(csv_reader)

        for row in csv_reader:
            #print(row)
            #print(row[0])
            #print(row[5])
            mcc_code_list.append((eval(row[0]),row[1],row[5]))
            #print(mcc_code_list)

    with open(cc_merchant_filename, 'r') as read_obj:
        csv_reader = reader(read_obj,delimiter="|")
        header = next(csv_reader)
        # Check file as empty
        if header != None:
            # Iterate over each row after the header in the csv
            count=0
            with open(merchant_output_filename, 'w', newline='') as merchantfile:
                merchant_fieldnames = ['merchant_id','merchant_name', 'mcc', 'email', 'street', 'city',
                               'state',  'country', 'zip', 'latitude', 'longitude','owner_id','owner_name','terminal_ids']
                merchant_writer = csv.DictWriter(
                merchantfile, delimiter='|', lineterminator='\n', fieldnames=merchant_fieldnames,  quotechar='"', doublequote=True)

                merchant_writer.writeheader()

                for row in csv_reader:
                    # row variable is a list that represents a row in csv
                    print(count)
                    count=count+1 
                    #for _ in range(merchant_per_mcc):

                    new_data = Merchant(row,count,mcc_code_list)
                    merchant_writer.writerow({
                        'merchant_id': new_data.merchant_id,
                        'merchant_name': new_data.merchant_name,
                        'mcc': new_data.mcc,
                        'email': new_data.email,
                        'street': new_data.street_address,
                        'city': new_data.city,
                        'state': new_data.state,
                        'country': new_data.country,
                        'zip': "",
                        'latitude': new_data.latitude,
                        'longitude': new_data.longitude,
                        'owner_id': new_data.owner_id,
                        'owner_name': new_data.owner_name,
                        'terminal_ids':new_data.terminal_ids
                    }
                    )

    print("Up-loading merchant and mcc  data")
    merchant_blob.upload_from_filename(merchant_output_filename)
    mcc_blob.upload_from_filename(merchant_mcc_codes)