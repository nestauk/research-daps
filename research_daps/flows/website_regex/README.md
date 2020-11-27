# Website Regex Flow

## Functionality
### Current

- Takes a new line delimited list of Seed URL's
- Discovers internal links (via. Selenium)
- Filters based on simple heuristics (limit to n, taking first n/2 and last n/2 links)
- Runs a regex to find twitter handles over all the URL's (counting the frequency of each)
- Reduces to provide a count for each network location

### Eventual

- Takes a new line delimited list of Seed URL's
- Discovers internal links (via. Selenium)
- Filters based on customisable heuristic function
- Runs a regex (custom function?)
- Runs a custom reducer over the output of the regex (custom function?) outputs

Best way to allow custom functions is to probably define flow as an abstract base class.

## Docker

`build_deploy_docker.sh` builds and deploys `Dockerfile` to AWS ECR so that selenium can be used with AWS batch.

Note: this adds the correct `METAFLOW_BATCH_CONTAINER_REGISTRY` to `.env` but this variable currently needs to be set manually when running the flow on AWS batch.

