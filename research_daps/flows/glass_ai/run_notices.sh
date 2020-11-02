#!/usr/bin/env bash
set -euo pipefail
while getopts ":t:" flag
do
    case "${flag}" in
        t) TEST=${OPTARG};;
        :) TEST=true
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

    s3_input=$S3_MOUNT'/data/raw/glass_ai_uk_2020JUN/covid_notices.2020JUN.txt'
    datastore=local
    metadata=local
else
    s3_input='s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/covid_notices.2020JUN.txt'
    datastore=s3
    metadata=service
fi

python notices.py\
    --no-pylint\
    --package-suffixes=.py,.txt\
    --metadata=$metadata\
    --datastore=$datastore\
    run\
    --s3_input=$s3_input\
    --date="05/2020"\
    # --with batch\
