source inputs.sh

mkdir -p ${local_data_dir}

python3 ./merchant_data/create_merchants.py ${num_merchants} ${merchant_seed} ${local_data_dir}/${merchant_filename} ${ref_data_local_path}/${mcc_file_name} ${local_data_dir}/${cc_merchant_file} ${merchant_project} ${merchant_bucket} ${merchant_gcs_filename} ${mcc_gcs_filename}

res=$?

if [ $res -eq 0 ];
then
#bq rm -t -f --project_id=${merchant_project} merchants_source_data.core_merchants
#bq rm -t -f --project_id=${merchant_project} merchants_reference_data.mcc_code


bq load \
--project_id=${merchant_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
--time_partitioning_field=date \
--time_partitioning_type=DAY \
--hive_partitioning_mode=AUTO \
--hive_partitioning_source_uri_prefix=gs://${merchant_bucket}/${cc_merchant_data} \
${merchant_bq_table} gs://${merchant_bucket}/${merchant_gcs_filename}

bq load \
--project_id=${merchant_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${mcc_bq_table} gs://${merchant_bucket}/${mcc_gcs_filename}
fi