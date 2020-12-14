"""Metaflow pipeline to run a regex over a set of websites."""
import json
import logging
import re
from itertools import chain
from typing import Collection, Dict, Iterable, List, Optional, Tuple, TypeVar

from metaflow import (
    conda_base,
    current,
    resources,
    retry,
    step,
    IncludeFile,
    FlowSpec,
    Parameter,
    S3,
)

T = TypeVar("T")


def joiner(regex_matches: List[Dict[str, Dict[str, int]]]) -> Dict[str, Dict[str, int]]:
    """Join `regex_matches` by summing the `int` counts on matching keys."""
    from toolz.curried import merge_with
    return merge_with(lambda x: merge_with(sum, x), regex_matches)


@conda_base(
    libraries={
        "toolz": "0.11.0",
        "beautifulsoup4": "4.9.3",
        "selenium": ">=3.141.0",
        "lxml": ">=4.5.1",
        "tenacity": ">=6.2.0",
    }
)
class WebsiteRegex(FlowSpec):
    """Metaflow pipeline to run a regex over a set of websites."""

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

    regex = Parameter(
        "regex",
        help="The regular expression to run over the collection of URLs",
        type=str,
        required=True,
    )

    def chunk_data(
        self, data: Collection[T], chunksize: Optional[int] = None
    ) -> Iterable[Tuple[T]]:
        """Split `data` into chunks of `chunksize`.

        Truncates data and the size of chunks if `self.test_mode`.
        """
        from toolz.curried import pipe, partition, take

        chunksize_ = (chunksize or len(data)) if not self.test_mode else 2

        logging.info(f"Data of length {len(data)} split into chunks of {chunksize_}")
        return pipe(
            data, partition(chunksize_), take(2 if self.test_mode else None)
        )

    @step
    def start(self):
        """Process and chunk seed URL's ready to be run in parallel batches."""

        self.seed_urls = self.seed_url_file.split("\n")
        self.url_chunks = list(self.chunk_data(self.seed_urls, self.chunksize))

        self.next(self.link_finder, foreach="url_chunks")

    @retry
    @resources(cpu=1)
    @step
    def link_finder(self):
        """Find internal links for each seed URL."""
        from utils import link_finder, Driver

        urls = self.input
        with Driver() as driver:
            self.links = list(
                chain.from_iterable(map(lambda x: link_finder(driver, x), urls))
            )

        self.next(self.join_link_finder)

    @step
    def join_link_finder(self, inputs):
        """Join found links from each batch into single list."""
        self.links = list(chain.from_iterable([input_.links for input_ in inputs]))
        self.next(self.regexer_dispatch)

    @step
    def regexer_dispatch(self):
        """Chunk and enumerate list of links ready to be run in parallel batches."""
        self.url_chunks_enumerated = list(
            enumerate(self.chunk_data(self.links, 250))
        )  # TODO : parameterise
        self.next(self.regexer, foreach="url_chunks_enumerated")

    @retry
    @resources(cpu=1)
    @step
    def regexer(self):
        """Run `self.regex` over each links page source.

        Only performs work for chunks not already completed.
        """
        from utils import match_finder, Driver

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
            regex = re.compile(self.regex)
            with Driver() as driver:
                self.handles = list(map(lambda x: match_finder(driver, regex, x), urls))

            with S3(s3root="s3://nesta-glass/twitter_handles/") as s3:
                s3.put(f"{run_id}/{chunk_number}", json.dumps(self.handles))
        else:
            with S3(s3root="s3://nesta-glass/twitter_handles/") as s3:
                self.handles = json.loads(s3.get(f"{run_id}/{chunk_number}").text)

        self.next(self.join_regexer)

    @step
    def join_regexer(self, inputs):
        """Join regex match counts from each batch into single dict."""
        regex_matches = list(chain.from_iterable((input_.handles for input_ in inputs)))
        self.regex_matches_by_domain = joiner(regex_matches)

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()],
        level=logging.INFO,
    )

    WebsiteRegex()
