cust_project="bankofmars-retail-customers"
cust_bucket="bankofmars_retail_customers_source_data"
local_data_path="/Users/maharanam/Desktop/data"
cust_gcs_file="customers_data/date=2022-01-01/customer.csv"
cc_cust_gcs_file="cc_customers_data/date=2022-01-01/cc_customer.csv"
seed=1

echo "python3 /Users/maharanam/OpenSourceCode/datamesh-datagenerator/customer_data/create_customers_cc_merchant.py 10 ${seed} ../profiles/main_config.json ${local_data_path}/customer.csv ${local_data_path}/cc_customer.csv ${local_data_path}/cc_merchant_info_for_trans.csv ${cust_project} ${cust_bucket} ${cust_gcs_file} ${cc_cust_gcs_file}"

create() {
if [ $? -ne 0 ];
then
bq rm -t -f --project_id=${cust_project} customers_source_data.customer_demographics
bq rm -t -f --project_id=${cust_project} customers_source_data.cc_customer_data


bq load \
--project_id=${cust_project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
customers_source_data.customer_demographics gs://${cust_bucket}/${cust_gcs_file}

bq load \
--project_id=${cust_project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
customers_source_data.cc_customer_data gs://${cust_bucket}/${cc_cust_gcs_file}
fi
}