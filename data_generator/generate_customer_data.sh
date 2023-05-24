source inputs.sh

mkdir -p ${local_data_dir}

echo "python3 ./customer_data/create_customers_cc_merchant.py ${num_of_customers} ${customer_seed} ${profile_name} ${local_data_dir}/${customer_file} ${local_data_dir}/${cc_customer_file} ${local_data_dir}/${cc_merchant_file} ${customer_project} ${customer_bucket} ${customer_gcs_file} ${cc_customer_map_gcs_file}"

python3 ./customer_data/create_customers_cc_merchant.py ${num_of_customers} ${customer_seed} ${profile_name} ${local_data_dir}/${customer_file} ${local_data_dir}/${cc_customer_file} ${local_data_dir}/${cc_merchant_file} ${customer_project} ${customer_bucket} ${customer_gcs_file} ${cc_customer_map_gcs_file}

res=$?

if [ $res -eq 0 ];
then
#bq rm -t -f --project_id=${customer_project} ${customer_bq_table}
#bq rm -t -f --project_id=${customer_project} ${cc_customer_map_bq_table}


echo "gs://${customer_bucket}/${customer_hive_parent}"
echo "gs://${customer_bucket}/${customer_gcs_file}"

bq load \
--project_id=${customer_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${customer_bq_table} gs://${customer_bucket}/${customer_gcs_file}

bq load \
--project_id=${customer_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_customer_map_bq_table} gs://${customer_bucket}/${cc_customer_map_gcs_file}
fi
