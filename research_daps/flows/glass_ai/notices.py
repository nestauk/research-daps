""" Metaflow pipeline to process raw Glass AI notices into dataframes """
import subprocess
import logging
import datetime
import sys

subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet', '-r', 'requirements.txt'])
import pandas as pd

from process_raw import (
    glass_df_loader,
    process_notice,
)
from metaflow import FlowSpec, Parameter, step


class GlassNoticesDumpFlow(FlowSpec):
    """ Clean and process raw Glass AI notices into dataframes """

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )

    s3_input = Parameter(
        "s3_input",
        help="S3 locations for notice data",
        type=str,
    )

    date = Parameter(
        "date",
        help="Datetime of data-dump. MM/YYYY",
        type=lambda x: datetime.datetime.strptime(x, '%m/%Y')
    )

    @step
    def start(self):
        """ Load raw data """
        logging.info("Loading raw data")

        print(self.date)
        print(pd.__version__)

        if self.test_mode:
            nrows = 10_000
            logging.warning(
                f"TEST MODE: Constraining to first {nrows} notices..."
            )
        else:
            nrows = None

        self.notices = glass_df_loader(self.s3_input, nrows=nrows)

        self.next(self.process)

    @step
    def process(self):
        """ Process tables """

        logging.info("Processing Notice")
        self.notices = process_notice(self.notices)
        self.next(self.end)

    @step
    def end(self):
        """ """
        pass


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()], level=logging.INFO,
    )

    GlassNoticesDumpFlow()
