#!/usr/bin/env bash
set -euo pipefail

repo_name="metaflow-graph-tool"  # TODO: $0

# Load AWS_ACCOUNT_ID from .env
export $(cat ../../../.env | xargs)

# Build image
sudo docker build -t $repo_name .

## Deploy to AWS ECR:
# See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/docker-basics.html
# Create repository if it doesn't exist
aws ecr list-images --repository-name $repo_name || aws ecr create-repository --repository-name $repo_name --region eu-west-2
# Tag image
sudo docker tag $repo_name $AWS_ACCOUNT_ID.dkr.ecr.eu-west-2.amazonaws.com/$repo_name
# Docker login to AWS ECR
aws ecr get-login-password | sudo docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.eu-west-2.amazonaws.com
# Push image to AWS ECR
sudo docker push $AWS_ACCOUNT_ID.dkr.ecr.eu-west-2.amazonaws.com/$repo_name
