Runs a TopSBM topic model on a large corpus of data. Done by request for Juan and documented here.

`build_docker.sh` builds and pushes to AWS ECR an image based on the graph-tool docker image (for simplicity and speed rather than messing with conda).

`sbmtm.py` contains the code to run a topSBM model and was obtained from https://github.com/martingerlach/hSBM_Topicmodel/blob/master/sbmtm.py (no package is available) and is bundled in with the Flow by metaflow.

The flow is in `topsbm.py` and requests a large amount of RAM in order to run the model.

**Don't run this flow with `--n-docs` as a large number as it will cost $$$**

