Research pipelines - DAPS
=========================

This repository is a DAPS for research pipelines required by multiple projects.

Unlike other DAPS, `research-daps` only makes use of metaflow "flows" directly, rather than wrapping these in a fully production ready Luigi "task".

Rationale for a research DAPS:
- Gives reusability of research pipelines and their outputs
- Decrease the complexity of project repositories - `metaflow` is the only dependency needed to access the results of `research-daps` pipelines
- Provide a pragmatic compromise between the rigours of prediction and the flexibility required for rapid prototyping
- Leverage (and contribute to) data and DAPS utilities shared between different DAPS

# Nesta users

## Configuration

First install `git-crypt` by [reading their docs](https://github.com/AGWA/git-crypt/blob/master/INSTALL.md).

Then:

```bash
    git clone git@github.com:nestauk/research-daps.git
    git-crypt unlock /path/to/research_daps.key
```
    
Note that `research_daps.key` can be found on S3 in the `nesta-production-config` bucket.

You should then configure Metaflow and AWS. To use the default settings (highly recommended), we advise against using Metaflow's configure script and instead simply to copy our config:

```bash
    cp -r research_daps/config/metaflowconfig ~/.metaflowconfig
```
    
After this you will need to install the `AWS CLI` by following [these instructions](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) and then run

```bash
    aws configure
```
    
and entering your credentials, noting that our default region is `eu-west-2` (which is London).

## Use

### Access results

Flow results can be accessed through the metaflow client API, and do not require installing `research_daps` directly. For detailed documentation, see the [official docs](https://docs.metaflow.org/metaflow/client).

# External Users

All pipelines in this repo are open; however some pipelines use proprietary data that Nesta has licensed and cannot share.

Should you wish to reproduce a pipeline that uses proprietary data which you would like to license or data whose origin is not clear, contact data@nesta.org.uk.

# Contributors

## Installation
    
Follow the configuration instructions for [Nesta users](#Configuration), and perform the following steps in addition:

```bash
    pip install -U --no-cache git+git://github.com/nestauk/daps_utils@dev
    bash install.sh
```

## Related DAPS projects

The private repo [`nestauk/daps_sandbox`](https://github.com/nestauk/daps_sandbox) is a testbed for prototyping new DAPS features. Generally speaking, almost all of these features will end up in the public repo [`nestauk/daps_utils`](https://github.com/nestauk/daps_utils). It is, of course, fine to prototype new pipeline features in this repository but a recommendation would be to highlight new features of this repo under [`daps_sandbox` issues](https://github.com/nestauk/daps_sandbox/issues), so that these features can be factored out and migrated to `daps_utils`.

In order to keep the integration tight, it is critical that this repo is as unit- and integration-tested as possible; otherwise, factoring out functionality from this repo could lead to significant headaches.

## Encryption

Any files under `research_daps/config` are automatically encrypted - put any sensitive information in there, although avoid commiting sensitive information entirely if possible.

## Contribution etiquette

After cloning the repo, you will need to run `bash install.sh` from the repository root. This will setup
automatic calendar versioning for you, and also some checks on your working branch name. For avoidance of doubt,
branches must be linked to a GitHub issue and named accordingly:

```bash
{GitHub issue number}_{tinyLowerCamelDescription}
```
For example `14_readme`, which indicates that [this branch](https://github.com/nestauk/research-daps/pull/24) refered to [this issue](https://github.com/nestauk/research-daps/issues/14).

The remote repo anyway forbids you from pushing directly to the `dev` branch, and the local repo will forbid you from committing to either `dev` or `master`, and so you only pull requests from branches named `{GitHub issue number}_{tinyLowerCamelDescription}` will be accepted.

Please make all PRs and issues reasonably small: they should be trying to achieve roughly one task. Inevitably some simple tasks spawn large numbers of utils, and sometimes these detract from the original PR. In this case, you should stack an new PR on top of your "base" PR, for example as `{GitHub issue number}_{differentLowerCamelDescription}`. In this case the PR / Git merging tree will look like:

    dev <-- 123_originalThing <-- 423_differentThing <-- 578_anotherDifferentThing
    
We can then merge the PR `123_originalThing` into `dev`, then `423_differentThing` into `dev` (after calling `git merge dev` on `423_differentThing`), etc until the chain is merged entirely. The nominated reviewer should review the entire chain, before the merge can go ahead. PRs should only be merged if all tests and a review has been signed off.

