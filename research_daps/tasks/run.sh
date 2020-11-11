#!/bin/bash


s3_inputs='{"organisation":"s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/organization.2020JUN.txt","address":"s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/address.2020JUN.txt","sector":"s3://nesta-glass/data/raw/glass_ai_uk_2020JUN/sectors.2020JUN.txt"}'
dump_date='05/2020'

PYTHON_PATH=~/GIT/research_daps python -m luigi --module research_daps.tasks.glass RootTask --dump-date $dump_date --s3-inputs "$s3_inputs"
