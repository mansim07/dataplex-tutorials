#!/usr/bin/env bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source ./inputs.sh

mkdir -p ${local_data_dir}

create_gcs_bucket() {
    # Signature: create_bucket PROJECT_ID,BUCKET,REGION
    # Description: creates a gcs bucket if it does not exists. This function takes three arguments:
    #   PROJECT_ID: The ID of the Google Cloud project that you want to create the bucket in.
    #   BUCKET: The name of the bucket that you want to create.
    #   REGION: The region where you want to create the bucket.

    echo "Creating bucket [$2] in region [$3] for project [$1]"

    project_id=${1}
    bucket=${2}
    region=${3}

    if ! gsutil ls -p ${project_id} gs://${bucket} &>/dev/null; then
        echo "Creating gs://${bucket} ... "
        gsutil mb -p ${project_id} -c regional -l ${region} gs://${bucket}
        sleep 5
    else
        echo "Bucket ${bucket} already exists!"
        echo "Please revise the bucket and delete manually or rerun the code"
    fi
}

create_bq_dataset() {
    # Signature: create_bq_dataset PROJECT_ID,DATASET,REGION
    # Sescription: creates a gcs bucket if it does not exists. This function takes three arguments:
    #   PROJECT_ID: The ID of the Google Cloud project that you want to create the bucket in.
    #   BUCKET: The name of the bucket that you want to create.
    #   REGION: The region where you want to create the bucket.
    project_id=$1
    dataset=$2
    region=$3

    exists=$(bq ls -d | { grep -w $dataset || :; })
    if [ -n "$exists" ]; then
        echo "Not creating $dataset since it already exists"
    else
        echo "Creating $dataset"
        bq --project_id=${project_id} --location=${region} mk -d ${dataset}
    fi
}

echo "========================================="
echo "Starting the data generation process"
echo "========================================="

# ============================================
# CREATE THE GCS BUCKETS
# ============================================
echo "Creating the customer raw data bucket."
create_gcs_bucket $PROJECT_ID ${customer_bucket} ${REGION}
echo "Customer raw data bucket successfully created."

echo "Creating the merchant raw data bucket."
create_gcs_bucket $PROJECT_ID ${merchant_bucket} ${REGION}
echo "Merchant raw data bucket successfully created."

echo "Creating the transaction raw data bucket."
create_gcs_bucket $PROJECT_ID ${transaction_bucket} ${REGION}
echo "Transaction raw data bucket successfully created."

# ============================================
# CREATE THE BQ datasets
# ============================================
echo "Creating the customer raw data dataset."
create_bq_dataset $PROJECT_ID ${customer_bq_ds} ${REGION}
echo "Customer raw data dataset successfully created."

echo "Creating the merchant raw data dataset."
create_bq_dataset $PROJECT_ID ${merchant_dq_ds} ${REGION}
create_bq_dataset $PROJECT_ID ${mcc_dq_ds} ${REGION}
echo "Merchant raw data dataset successfully created."

echo "Creating the transaction raw data dataset."
create_bq_dataset $PROJECT_ID ${auth_bq_ds} ${REGION}
create_bq_dataset $PROJECT_ID ${cc_ref_bq_dataset} ${REGION}
echo "Transaction raw data dataset successfully created."

#=============================================
# GENERATE AND STAGE THE CUSTOMER DATA
#=============================================
echo "Generating the customer demographics and profiling data using the below code"
echo "python3 ./customer_data/create_customers_cc_merchant.py ${num_of_customers} ${customer_seed} ${profile_name} ${local_data_dir}/${customer_file} ${local_data_dir}/${cc_customer_file} ${local_data_dir}/${cc_merchant_file} ${customer_project} ${customer_bucket} ${customer_gcs_file} ${cc_customer_map_gcs_file}"

python3 ./customer_data/create_customers_cc_merchant.py ${num_of_customers} ${customer_seed} ${profile_name} ${local_data_dir}/${customer_file} ${local_data_dir}/${cc_customer_file} ${local_data_dir}/${cc_merchant_file} ${customer_project} ${customer_bucket} ${customer_gcs_file} ${cc_customer_map_gcs_file}

res=$?

if [ $res -eq 0 ]; then
    echo "Customer data was successfully generated and uploaded to Google Cloud storage successfully."
    echo "Now loading the customer data from Google cloud storage to BQ.."

    bq load \
        --project_id=${customer_project} \
        --replace \
        --autodetect \
        --source_format=CSV \
        --field_delimiter="|" \
        --skip_leading_rows=1 \
        --allow_quoted_newlines \
        --allow_jagged_rows \
        ${customer_bq_ds}.${customer_bq_table} gs://${customer_bucket}/${customer_gcs_file}

    bq load \
        --project_id=${customer_project} \
        --replace \
        --autodetect \
        --source_format=CSV \
        --field_delimiter="|" \
        --skip_leading_rows=1 \
        --allow_quoted_newlines \
        --allow_jagged_rows \
        ${customer_bq_ds}.${cc_customer_map_bq_table} gs://${customer_bucket}/${cc_customer_map_gcs_file}
fi

echo "Successfully loaded the customer data from storage bucket to bq"

echo "Generating the customer demographics and profiling data is completed successfully."

#=============================================
# GENERATE AND STAGE THE MERCHANT DATA
#=============================================
echo "Generating the Merchant demographics data using the below code"
echo "python3 ./merchant_data/create_merchants.py ${num_merchants} ${merchant_seed} ${local_data_dir}/${merchant_filename} ${ref_data_local_path}/${mcc_file_name} ${local_data_dir}/${cc_merchant_file} ${merchant_project} ${merchant_bucket} ${merchant_gcs_filename} ${mcc_gcs_filename}"

python3 ./merchant_data/create_merchants.py ${num_merchants} ${merchant_seed} ${local_data_dir}/${merchant_filename} ${ref_data_local_path}/${mcc_file_name} ${local_data_dir}/${cc_merchant_file} ${merchant_project} ${merchant_bucket} ${merchant_gcs_filename} ${mcc_gcs_filename}

res=$?

if [ $res -eq 0 ]; then

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
        ${merchant_dq_ds}.${merchant_bq_table} gs://${merchant_bucket}/${merchant_gcs_filename}

    bq load \
        --project_id=${merchant_project} \
        --replace \
        --autodetect \
        --source_format=CSV \
        --field_delimiter="|" \
        --skip_leading_rows=1 \
        --allow_quoted_newlines \
        --allow_jagged_rows \
        ${mcc_dq_ds}.${mcc_bq_table} gs://${merchant_bucket}/${mcc_gcs_filename}
fi

#=============================================
# GENERATE AND STAGE THE TRANSACTION DATA
#=============================================
echo "Generating the transaction data using the below code"
echo "python3 ./transaction_data/create_transactions.py ${num_of_trans_per_cust} ${cc_trans_seed} ${local_data_dir}/${cc_merchant_file} ${local_data_dir}/${trans_filename} ${start_date} ${end_date} ${transaction_project} ${transaction_bucket} ${cc_auth_gcs_path}"

python3 ./transaction_data/create_transactions.py ${num_of_trans_per_cust} ${cc_trans_seed} ${local_data_dir}/${cc_merchant_file} ${local_data_dir}/${trans_filename} ${start_date} ${end_date} ${transaction_project} ${transaction_bucket} ${cc_auth_gcs_path}

res=$?

if [ $res -eq 0 ]; then
    echo "Generating and uploading transaction reference data "
    python3 ./transaction_data/upload_ref_data.py ${transaction_project} ${transaction_bucket} ${trans_ref_data}

    res1=$?

    if [ $res1 -eq 0 ]; then

        bq load \
            --project_id=${transaction_project} \
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
            --hive_partitioning_source_uri_prefix=gs://${transaction_bucket}/${cc_hive_parent} \
            ${auth_bq_ds}.${auth_bq_table} gs://${transaction_bucket}/${cc_auth_gcs_path}

        bq load \
            --project_id=${transaction_project} \
            --replace \
            --autodetect \
            --source_format=CSV \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.card_read_type gs://${transaction_bucket}/ref_data/card_read_type/card_read_type.csv

        bq load \
            --project_id=${transaction_project} \
            --replace \
            --autodetect \
            --source_format=CSV \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.card_type_facts gs://${transaction_bucket}/ref_data/card_type_facts/card_type_facts.csv

        bq load \
            --project_id=${transaction_project} \
            --replace \
            --autodetect \
            --source_format=CSV \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.currency gs://${transaction_bucket}/ref_data/currency/currency.csv

        bq load \
            --project_id=${transaction_project} \
            --replace \
            --autodetect \
            --source_format=CSV \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.events_type gs://${transaction_bucket}/ref_data/events_type/events_type.csv

        bq load \
            --project_id=${transaction_project} \
            --replace \
            --autodetect \
            --source_format=CSV \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.origination_code gs://${transaction_bucket}/ref_data/origination_code/origination_code.csv

        bq load \
            --project_id=${transaction_project} \
            --replace \
            --autodetect \
            --source_format=CSV \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.payment_methods gs://${transaction_bucket}/ref_data/payment_methods/payment_methods.csv

        bq load \
            --project_id=${transaction_project} \
            --autodetect \
            --source_format=CSV \
            --replace \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.signature gs://${transaction_bucket}/ref_data/signature/signature.csv

        bq load \
            --project_id=${transaction_project} \
            --autodetect \
            --source_format=CSV \
            --replace \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.swiped_code gs://${transaction_bucket}/ref_data/swiped_code/swiped_code.csv

        bq load \
            --project_id=${transaction_project} \
            --autodetect \
            --source_format=CSV \
            --replace \
            --field_delimiter="|" \
            --skip_leading_rows=1 \
            --allow_quoted_newlines \
            --allow_jagged_rows \
            ${cc_ref_bq_dataset}.trans_type gs://${transaction_bucket}/ref_data/trans_type/trans_type.csv

    fi

fi
