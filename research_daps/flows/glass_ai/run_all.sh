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
    s3fs nesta-glass $S3_MOUNT -o passwd_file=$HOME/.passwd-s3fs -o use_cache=/tmp || true

    datastore=local
    metadata=local
else
    # Mount s3 bucket with s3fs (cache downloading of input files for dev)
    S3_MOUNT=/media/s3fs
    mkdir -p $S3_MOUNT
    chown $USER $S3_MOUNT || true
    s3fs nesta-glass $S3_MOUNT -o passwd_file=$HOME/.passwd-s3fs -o use_cache=/tmp || true
    # S3_MOUNT=s3://nesta-glass

    datastore=s3
    metadata=service
fi

datastore=s3
metadata=service

# s3_input='{"organisation": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/organization.2020JUN.txt", "address": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/address.2020JUN.txt", "sector": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/sectors.2020JUN.txt"}'
# python ingest_data_dump.py\
#     --no-pylint\
#     --datastore=$datastore\
#     --metadata=$metadata\
#     run\
#     --run-id-file=.run_id_JUN\
#     --s3_inputs="$s3_input"\
#     --test_mode="$TEST"\
#     --date="06/2020"

s3_input='{"organisation": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUL/organization.2020JUL.txt", "address": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUL/address.2020JUL.txt", "sector": "'$S3_MOUNT'/data/raw/glass_ai_uk_2020JUL/sectors.2020JUL.txt"}'
python ingest_data_dump.py\
    --no-pylint\
    --datastore=$datastore\
    --metadata=$metadata\
    run\
    --run-id-file=.run_id_JUL\
    --s3_inputs="$s3_input"\
    --test_mode="$TEST"\
    --date="07/2020"


