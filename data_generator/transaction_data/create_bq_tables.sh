project="bankofmars-retail-credit-cards"
bucket="bankofmars_retail_credit_cards_trasactions_data"

bq rm -t -f --project_id=${project} lookup_data.card_read_type
bq rm -t -f --project_id=${project} lookup_data.card_type_facts
bq rm -t -f --project_id=${project} lookup_data.currency
bq rm -t -f --project_id=${project} lookup_data.events_type
bq rm -t -f --project_id=${project} lookup_data.origination_code
bq rm -t -f --project_id=${project} lookup_data.payment_methods
bq rm -t -f --project_id=${project} lookup_data.signature
bq rm -t -f --project_id=${project} lookup_data.swiped_code
bq rm -t -f --project_id=${project} lookup_data.trans_type




bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.card_read_type gs://${bucket}/ref_data/card_read_type.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.card_type_facts gs://${bucket}/ref_data/card_type_facts.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.currency gs://${bucket}/ref_data/currency.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.events_type gs://${bucket}/ref_data/events_type.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.origination_code gs://${bucket}/ref_data/origination_code.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.payment_methods gs://${bucket}/ref_data/payment_methods.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.signature gs://${bucket}/ref_data/signature.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.swiped_code gs://${bucket}/ref_data/swiped_code.csv

bq load \
--project_id=${project} \
--autodetect \
--source_format=CSV \
--field_delimiter="|" \
--skip_leading_rows=1 \
--allow_quoted_newlines \
--allow_jagged_rows \
lookup_data.trans_type gs://${bucket}/ref_data/trans_type.csv