#!/bin/bash 

NUM_PAR=`expr $1 - 1`
cd ~/dataplex-tutorials/data_generator
for dt in $(eval echo "{0..$NUM_PAR}")
do
    export DATE_PARTITION=$( date -d"+${dt} days" +%Y-%m-%d )
    echo "Generating Data for Partition $DATE_PARTITION"
    bash generate_sample_data.sh
done