#!/usr/bin/env bash
set -euo pipefail
TEST=true
while getopts ":p" flag
do
    case "${flag}" in
        p) TEST=false;;
    esac
done

echo Test? $TEST
if $TEST
then
    # Mount s3 bucket with s3fs (cache downloading of input files for dev)
    S3_MOUNT=/media/s3fs
    mkdir -p $S3_MOUNT
    chown $USER $S3_MOUNT || true
    s3fs nesta-glass $S3_MOUNT -o passwd_file=.passwd-s3fs -o use_cache=/tmp || true

    s3_input='{"organisation": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/organization.2020JUN.txt", "address": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/address.2020JUN.txt", "sector": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/sectors.2020JUN.txt"}'
    datastore=local
    metadata=local
else
    s3_input='{"organisation": "s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/organization.2020JUN.txt", "address": "s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/address.2020JUN.txt", "sector": "s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/sectors.2020JUN.txt"}'
    datastore=s3
    metadata=service
fi

metadata=service
python ingest_data_dump.py\
    --no-pylint\
    --datastore=$datastore\
    --metadata=$metadata\
    run\
    --s3_inputs="$s3_input"\
    --test_mode="$TEST"\
    --date="05/2020"\
