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
    # S3_MOUNT=/media/s3fs
    # mkdir -p $S3_MOUNT
    # chown $USER $S3_MOUNT || true
    # s3fs nesta-glass $S3_MOUNT -o passwd_file=$HOME/.passwd-s3fs -o use_cache=/tmp || true

    datastore=local
    metadata=local
    flow_ids='[397, 398]'
else
    # S3_MOUNT=s3://
    datastore=s3
    metadata=service
    flow_ids='[402, 400]'
fi

echo FLOWS: $flow_ids

python merge_flow.py\
    --no-pylint\
    --datastore=$datastore\
    --metadata=$metadata\
    run\
    --run-id-file=.run_id_JUN\
    --flow_ids="$flow_ids"\
    --test_mode="$TEST"\