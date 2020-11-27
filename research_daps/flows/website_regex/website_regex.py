"""Metaflow pipeline to run regexes over websites."""
import logging
import os
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
    # Run,
    FlowSpec,
    IncludeFile,
    Parameter,
    step,
    JSONType,
    # S3,
    # resources,
)

from utils import chrome_driver, link_finder, handle_finder


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

    def chunk_data(self, data):
        if self.chunksize is None:
            chunksize_ = len(self.seed_urls_)
        else:
            chunksize_ = self.chunksize

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

        print(self.seed_urls)

        self.url_chunks = list(self.chunk_data(self.seed_urls))

        self.next(self.link_finder, foreach="url_chunks")

    @step
    def link_finder(self):
        pip_install()
        urls = self.input
        with chrome_driver() as driver:
            self.links = list(
                chain.from_iterable(map(lambda x: link_finder(driver, x), urls))
            )

        self.next(self.join_link_finder)

    @step
    def join_link_finder(self, inputs):
        self.links = list(chain.from_iterable([input_.links for input_ in inputs]))
        print(self.links)
        self.next(self.regexer_dispatch)

    @step
    def regexer_dispatch(self):
        self.url_chunks = list(self.chunk_data(self.links))
        self.next(self.regexer, foreach="url_chunks")

    @step
    def regexer(self):
        pip_install()
        urls = self.input
        with chrome_driver() as driver:
            self.handles = list(map(lambda x: handle_finder(driver, x), urls))

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
