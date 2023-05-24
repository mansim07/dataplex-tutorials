
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
from datetime import timezone, datetime, timedelta
import csv
#from faker_credit_score import CreditScore
import os
import glob
from csv import reader
import time
from google.cloud import storage
import json


class Transactions:
   
    def __init__(self, row, counter,start_date_obj,end_date_obj):
        self.cc_token=row[3]
        self.card_read_type=5
        date_time= fake.date_time_between(start_date=start_date_obj,end_date=end_date_obj) #fake.date_time_between(start_date='-3d',end_date='now')
        self.trans_start_ts=time.mktime(date_time.timetuple())
        rand_secs=random.choice([7,8,9,10])
        end_time=date_time + timedelta(seconds=rand_secs)
        self.trans_end_ts=time.mktime(end_time.timetuple())

        #self.trans_time=fake.time()
        self.trans_type=1
        self.trans_amount=fake.pricetag().replace('$','')
        self.trans_currency="USD"
        self.trans_auth_code=fake.random_number(digits=6,fix_len=True)
        self.trans_auth_date= time.mktime(fake.date_time_between(start_date=date_time, end_date=end_time).timetuple())#self.trans_ts
        self.payment_method=random.choice([1,2,3,4,5,6,7,8,9,10,11])
        self.origination=1
        self.is_pin_entry=random.choice([0,1])
        self.is_signed=random.choice([0,1])
        self.is_unattended=random.choice([0,1])
        self.swipe_type=random.choice([0,1])
        self.merchant_id=row[0]  #random.choice(terminal_list)
        self.event_ids=fake.uuid4()
        self.event=""
 

    def get_merchant_name(self,row):
        if row[5] == 'Y':
            return row[1]
        else:
            return fake.company() + " " + row[1].split(' ')[0] + " " + fake.company_suffix()



def validat_parse_input():
    def print_err(n):
        if n == 1:
            print('Error: invalid number of transactions per customer')
        elif n == 2:
            print('Error: invalid (non-integer) random seed')
        elif n == 3:
            print('Error: specified input file cannot be opened')
        elif n == 4:
            print('Error: Missing/Invalid output file name')
        elif n == 5:
            print('Error: invalid start date')
        elif n == 6:
            print('Error: invalid end date')
        elif n == 7:
            print('Error: invalid project id')
        elif n == 8:
            print('Error: invalid bucket name')
        elif n == 9:
            print('Error: invalid GCS file name')


        output = '\nENTER:\n (1) Number of transactions per customer\n '
        output += '(2) Random seed (int)\n '
        output += '(3) input file containing customer token and merchant info \n'
        output += '(4) Local transaction file output name\n'
        output += '(5) Start date\n'
        output += '(6) End date\n'
        output += '(7) GCS project name\n'
        output += '(8) GCS bucket_name\n'
        output += '(9) GCS output file name\n'

        print(output)
        sys.exit(1)

    try:
        num_trans_per_customer = int(sys.argv[1])
    except:
        print_err(1)
    try:
        seed_num = int(sys.argv[2])
    except:
        print_err(2)
    try:
        cc_merchant_file = sys.argv[3]
        #merchant_output_filename = open(m, 'r').read()
    except:
        print_err(3)
    try:
        transactions_output_filename = sys.argv[4]
    except:
        print_err(4)
    try:
        start_date = sys.argv[5]
    except:
        print_err(5)

    try:
        end_date = sys.argv[6]
    except:
        print_err(6)
    try:
        project_id = sys.argv[7]
    except:
        print_err(7)
    try:
        bucket_name = sys.argv[8]
    except:
        print_err(8)
    try:
        transaction_file = sys.argv[9]
    except:
        print_err(9)

    return  num_trans_per_customer, seed_num, cc_merchant_file, transactions_output_filename, start_date, end_date,  project_id, bucket_name, transaction_file

if __name__ == '__main__':
    #create transaction and transaction history
    num_trans_per_customer, seed_num, cc_merchant_file, transactions_output_filename, start_date, end_date,  project_id, bucket_name, transaction_file= validat_parse_input()

    print("Generating Transaction Data")
    fake = Faker()
    Faker.seed(seed_num)

    start_date_obj=datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_obj=datetime.strptime(start_date, '%Y-%m-%d').date()
    ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    transaction_blob = bucket.blob(transaction_file)
  
    ''' 
    terminal_list=[]
    with open(merchant_file_name, 'r') as read_obj:
        csv_reader = reader(read_obj,delimiter='|')
        header = next(csv_reader)

        for row in csv_reader:
            terminal_list.append(eval(row[13])[0])
    '''
    for _ in range(num_trans_per_customer):
        with open(cc_merchant_file, 'r') as read_obj:
            csv_reader = reader(read_obj,delimiter='|')
            header = next(csv_reader)
            # Check file as empty
            if header != None:
                # Iterate over each row after the header in the csv
                count=0
                with open(transactions_output_filename, 'w', newline='',encoding='utf-8') as transactionfile:
                    #transactions_fieldnames = ['cc_token','card_read_type', 'trans_start_ts','trans_end_ts', 'trans_type', 'trans_amount',
                    #            'trans_currency',  'trans_auth_code', 'trans_auth_date', 'payment_method', 'origination','is_pin_entry', 'is_signed','is_unattended','swipe_type', 'merchant_id','event_ids','event']

                    #transaction_writer = csv.DictWriter(
                    #transactionfile, delimiter='|', lineterminator='\n', fieldnames=transactions_fieldnames,  quotechar='"', doublequote=True)

                    #transaction_writer.writeheader()

                    for row in csv_reader:
                        # row variable is a list that represents a row in csv
                        count=count+1 
                        
                        new_data = Transactions(row,count,start_date_obj,end_date_obj)
                        
                        data={
                            'cc_token': new_data.cc_token,
                            'card_read_type': new_data.card_read_type,
                            'trans_start_ts': new_data.trans_start_ts,
                            'trans_end_ts': new_data.trans_end_ts,
                            'trans_type': new_data.trans_type,
                            'trans_amount': new_data.trans_amount,
                            'trans_currency': new_data.trans_currency,
                            'trans_auth_code': new_data.trans_auth_code,
                            'trans_auth_date': new_data.trans_auth_date,
                            'payment_method': new_data.payment_method,
                            'origination': new_data.origination,
                            'is_pin_entry': new_data.is_pin_entry,
                            'is_signed': new_data.is_signed,
                            'is_unattended':new_data.is_unattended,
                            'swipe_type':new_data.swipe_type,
                            'merchant_id': new_data.merchant_id,
                            'event_ids':new_data.event_ids,
                            'event':new_data.event

                            }
                        
                        transactionfile.write(json.dumps(data))
                        transactionfile.write('\n')

                        #json.dump(new_data, f, ensure_ascii=False, indent=4)

                        
                        

                        #if count == num_trans_per_customer: 
                        #   break
    print("Uploading Transaction Data to GCS")
    transaction_blob.upload_from_filename(transactions_output_filename)


