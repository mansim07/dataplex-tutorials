#date value for gcs path 
#date_partition=2020-10-10
date_partition=`date "+%Y-%m-%d"`
local_data_dir="/tmp/data/"

#Customer Inputs
customer_project=${PROJECT_ID}
customer_bucket="${PROJECT_ID}_customers_raw_data"
customer_bq_ds="customer_raw_data"
customer_bq_table="customer_demographics"
cc_customer_map_bq_table="customer_credit_card_profile"
customer_seed=10
num_of_customers=2000
profile_name="./profiles/main_config.json"
customer_hive_parent="customers_data"
cc_customer_parent="customer_credit_card_profile"
customer_file=customer.csv
cc_customer_file=cc_customer.csv
cc_merchant_file=cc_merchant_info_for_trans.csv
customer_gcs_file="${customer_hive_parent}/dt=${date_partition}/${customer_file}"
cc_customer_map_gcs_file="${cc_customer_parent}/dt=${date_partition}/${cc_customer_file}"

#Merchant Inputs
merchant_project=${PROJECT_ID}
merchant_bucket="${PROJECT_ID}_merchants_raw_data"
merchant_seed=20
num_merchants=1000
cc_merchant_data="merchants_data"
ref_data_local_path="./merchant_data/data/ref_data"
mcc_file_name="mcc_codes.csv"
merchant_filename="merchants.csv"
merchant_gcs_filename="${cc_merchant_data}/date=${date_partition}/${merchant_filename}"
mcc_gcs_filename="mcc_codes/date=${date_partition}/${mcc_file_name}"
#make sure "cc_merchant_file" is set
merchant_dq_ds="merchant_raw_data"
mcc_dq_ds="merchants_reference_data"
merchant_bq_table="core_merchants"
mcc_bq_table="mcc_code"


#Credit Card Transactions Inputs
transaction_project=${PROJECT_ID}
transaction_bucket="${PROJECT_ID}_trasactions_data"
cc_ref_bq_dataset="lookup_data"
cc_hive_parent="auth_data"
num_of_trans_per_cust=3
cc_trans_seed=30
trans_filename=transaction_data.csv
cc_auth_gcs_path="${cc_hive_parent}/date=${date_partition}/${trans_filename}"
start_date=`date "+%Y-%m-%d"`
end_date=`date '+%Y-%m-%d' -d "$start_date+10 days"`
#end_date=`date "+%Y-%m-%d"`
trans_ref_data="./transaction_data/data/ref_data"
auth_bq_ds="transaction_raw_data"
auth_bq_table="auth_table"