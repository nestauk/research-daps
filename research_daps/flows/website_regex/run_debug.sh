export METAFLOW_BATCH_CONTAINER_REGISTRY=195787726158.dkr.ecr.eu-west-2.amazonaws.com; python website_regex.py --datastore local --metadata local --package-suffixes=.py,.txt run --seed-url-file seed_urls.txt --test_mode false --chunksize 10000 --max-workers 1

