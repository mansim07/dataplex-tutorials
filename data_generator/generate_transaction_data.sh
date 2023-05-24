source inputs.sh

mkdir -p ${local_data_dir}

echo "python3 ./transaction_data/create_transactions.py ${num_of_trans_per_cust} ${cc_trans_seed} ${local_data_dir}/${cc_merchant_file} ${local_data_dir}/${trans_filename} ${start_date} ${end_date} ${credit_card_project} ${credit_card_bucket} ${cc_auth_gcs_path}"

python3 ./transaction_data/create_transactions.py ${num_of_trans_per_cust} ${cc_trans_seed} ${local_data_dir}/${cc_merchant_file} ${local_data_dir}/${trans_filename} ${start_date} ${end_date} ${credit_card_project} ${credit_card_bucket} ${cc_auth_gcs_path}

res=$?

if [ $res -eq 0 ];
then

python3 ./transaction_data/upload_ref_data.py ${credit_card_project} ${credit_card_bucket} ${trans_ref_data}

res1=$?

if [ $res1 -eq 0 ];
then

bq load \
--project_id=${credit_card_project} \
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
--hive_partitioning_source_uri_prefix=gs://${credit_card_bucket}/${cc_hive_parent} \
${auth_bq_table} gs://${credit_card_bucket}/${cc_auth_gcs_path}

bq load \
--project_id=${credit_card_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.card_read_type gs://${credit_card_bucket}/ref_data/card_read_type/card_read_type.csv

bq load \
--project_id=${credit_card_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.card_type_facts gs://${credit_card_bucket}/ref_data/card_type_facts/card_type_facts.csv

bq load \
--project_id=${credit_card_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.currency gs://${credit_card_bucket}/ref_data/currency/currency.csv

bq load \
--project_id=${credit_card_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.events_type gs://${credit_card_bucket}/ref_data/events_type/events_type.csv

bq load \
--project_id=${credit_card_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.origination_code gs://${credit_card_bucket}/ref_data/origination_code/origination_code.csv

bq load \
--project_id=${credit_card_project} \
--replace \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.payment_methods gs://${credit_card_bucket}/ref_data/payment_methods/payment_methods.csv

bq load \
--project_id=${credit_card_project} \
--autodetect \
--source_format=CSV \
--replace \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.signature gs://${credit_card_bucket}/ref_data/signature/signature.csv

bq load \
--project_id=${credit_card_project} \
--autodetect \
--source_format=CSV \
--replace \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.swiped_code gs://${credit_card_bucket}/ref_data/swiped_code/swiped_code.csv

bq load \
--project_id=${credit_card_project} \
--autodetect \
--source_format=CSV \
--replace \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
${cc_ref_bq_dataset}.trans_type gs://${credit_card_bucket}/ref_data/trans_type/trans_type.csv

fi 

fi




