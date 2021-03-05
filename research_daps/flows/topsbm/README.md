Runs a TopSBM topic model (https://topsbm.github.io/) on a corpus of data.

# Usage


## Direct

`python topsbm.py run --input-file <your_documents.json> --n-docs -1 --seed 42 --with batch:image=metaflow-graph-tool`

To see help for parameters run: `python topsbm.py run --help`

## `research_daps` as a "library"

```python
TODO
```

# Developers

`build_docker.sh` builds and pushes to AWS ECR an image based on the graph-tool docker image (for simplicity and speed rather than messing with conda).

`sbmtm.py` contains the code to run a topSBM model and was obtained from https://github.com/martingerlach/hSBM_Topicmodel/blob/master/sbmtm.py (no package is available) and is bundled in with the Flow by metaflow.
TODO: modifications?
TODO: nlp-utils
TODO: required by end-user

`topsbm.py` contains the flow itself. 
