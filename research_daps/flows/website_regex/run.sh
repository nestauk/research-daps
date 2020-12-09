export METAFLOW_BATCH_CONTAINER_REGISTRY=195787726158.dkr.ecr.eu-west-2.amazonaws.com; python website_regex.py --package-suffixes=.py,.txt --no-pylint run --seed-url-file seed_urls.txt --test_mode false --chunksize 1000 --max-workers 12 # --with batch:cpu=1,image=pyselenium

