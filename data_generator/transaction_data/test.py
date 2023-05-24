

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
from faker_credit_score import CreditScore
import os
import glob
from csv import reader
import datetime


terminal_list=[]
print((datetime.datetime.now() - datetime.timedelta(1)).strftime("%s"))
#with open("/Users/maharanam/OpenSourceCode/datamesh-datagenerator/merchant_data/data/merchant.csv", 'r') as read_obj:
#        csv_reader = reader(read_obj,delimiter='|')
#        header = next(csv_reader)

#        for row in csv_reader:
#            terminal_list.append(eval(row[13])[0])
#        print(sys.getsizeof(terminal_list))