export METAFLOW_BATCH_CONTAINER_REGISTRY=195787726158.dkr.ecr.eu-west-2.amazonaws.com;
python website_regex.py --no-pylint --package-suffixes=.py,.txt resume\
 --with batch:image=pyselenium,queue=job-queue-many-nesta-metaflow\
 --max-workers 32 --origin-run-id 666 --max-num-splits 1000

