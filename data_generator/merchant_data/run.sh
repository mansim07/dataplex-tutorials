merchant_project="bankofmars-retail-merchants"
merchant_bucket="bankofmars_retail_merchants_data"
local_data_path="/Users/maharanam/OpenSourceCode/datamesh-datagenerator/customer_data/data/"

mcc_file_name="/Users/maharanam/OpenSourceCode/datamesh-datagenerator/merchant_data/data/mcc_codes.csv"
cc_merchant_filename=${local_data_path}/cc_merchant_info_for_trans.csv
seed=1
merchant_gcs_filename="merchants_data/date=2020-10-10/merchants.csv"
mcc_gcs_filename="mcc_codes/date=2020-10-10/mcc_code.csv"


python3 /Users/maharanam/OpenSourceCode/datamesh-datagenerator/merchant_data/create_merchants.py 10000 ${seed} ${local_data_path}/merchant.csv ${mcc_file_name} ${cc_merchant_filename} ${merchant_project} ${merchant_bucket} ${merchant_gcs_filename} ${mcc_gcs_filename}

#if [ $? -ne 0 ];
#then
bq rm -t -f --project_id=${merchant_project} merchants_source_data.core_merchants
bq rm -t -f --project_id=${merchant_project} merchants_reference_data.mcc_code


bq load \
--project_id=${merchant_project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
merchants_source_data.core_merchants gs://${merchant_bucket}/${merchant_gcs_filename}

bq load \
--project_id=${merchant_project} \
--autodetect \
--source_format=CSV \
--field_delimiter="," \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
merchants_reference_data.mcc_code gs://${merchant_bucket}/${mcc_gcs_filename}
#fi