"""Metaflow pipeline to run regexes over websites."""
import json
import logging
import subprocess
import sys
from pathlib import Path
from itertools import chain
from typing import Dict, List


def pip_install():
    path = str(Path(__file__).parents[0] / "requirements.txt")
    output = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", path], capture_output=True
    )
    if output.stderr:
        print("Install errors:", output.stderr)
    return output


pip_install()

import toolz.curried as t
from metaflow import (
    FlowSpec,
    IncludeFile,
    Parameter,
    step,
    JSONType,
    resources,
    retry,
    S3,
    current,
)

from utils import link_finder, handle_finder, Driver


def joiner(regex_matches: List[Dict[str, Dict[str, int]]]) -> Dict[str, Dict[str, int]]:
    return t.merge_with(lambda x: t.merge_with(sum, x), regex_matches)


class WebsiteRegex(FlowSpec):
    seed_url_file = IncludeFile(
        "seed-url-file",
        help="Newline delimited URL seed list",
    )

    chunksize = Parameter(
        "chunksize",
        help="Number of parameters to query with each batch machine",
        type=int,
        default=1_00,
    )

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (fetch a subset of data)",
        type=bool,
        default=True,
    )

    def chunk_data(self, data, chunksize=None):
        if chunksize is None:
            chunksize_ = len(self.seed_urls_)
        else:
            chunksize_ = chunksize

        if self.test_mode:
            chunksize_ = 2

        print(f"Data of length {len(data)} split into chunks of {chunksize_}")
        return t.partition(chunksize_, data)

    @step
    def start(self):
        """ """
        pip_install()

        self.seed_urls = self.seed_url_file.split("\n")

        if self.test_mode:
            self.seed_urls = list(t.take(4, self.seed_urls))

        self.url_chunks = list(self.chunk_data(self.seed_urls, self.chunksize))

        self.next(self.link_finder, foreach="url_chunks")

    @retry
    @resources(cpu=1)
    @step
    def link_finder(self):
        pip_install()
        urls = self.input
        with Driver() as driver:
            self.links = list(
                chain.from_iterable(map(lambda x: link_finder(driver, x), urls))
            )

        self.next(self.join_link_finder)

    @step
    def join_link_finder(self, inputs):
        self.links = list(chain.from_iterable([input_.links for input_ in inputs]))
        self.next(self.regexer_dispatch)

    @step
    def regexer_dispatch(self):
        pip_install()
        self.url_chunks_enumerated = list(
            enumerate(self.chunk_data(self.links, 250))
        )  # TODO : parameterise
        self.next(self.regexer, foreach="url_chunks_enumerated")

    @retry
    @resources(cpu=1)
    @step
    def regexer(self):
        pip_install()
        chunk_number, urls = self.input

        run_id = str(current.origin_run_id or current.run_id)
        print("Run ID:", run_id)

        with S3(s3root="s3://nesta-glass/twitter_handles/") as s3:
            completed_chunks = [int(x.key) for x in s3.list_paths([run_id])]
            if chunk_number in completed_chunks:
                completed_already = True
            else:
                completed_already = False

        if not completed_already:
            with Driver() as driver:
                self.handles = list(map(lambda x: handle_finder(driver, x), urls))

            with S3(s3root="s3://nesta-glass/twitter_handles/") as s3:
                s3.put(
                    f"{run_id}/{chunk_number}", json.dumps(self.handles)
                )
        else:
            with S3(s3root="s3://nesta-glass/twitter_handles/") as s3:
                self.handles = json.loads(
                    s3.get(f"{run_id}/{chunk_number}").text
                )

        self.next(self.join_regexer)

    @step
    def join_regexer(self, inputs):
        pip_install()
        regex_matches = list(chain.from_iterable((input_.handles for input_ in inputs)))
        self.regex_matches_by_domain = joiner(regex_matches)

        self.next(self.end)

    @step
    def end(self):
        # pip_install()
        pass


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()],
        level=logging.INFO,
    )

    WebsiteRegex()
