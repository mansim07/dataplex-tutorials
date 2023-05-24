#!/bin/bash 

cd dataplex-tutorials/data_generator
for dt in {0..4}
do
    export DATE_PARTITION=$( date -d"+${dt} days" +%Y-%m-%d )
    bash generate_sample_data.sh
done