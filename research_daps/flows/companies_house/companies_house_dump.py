""" Metaflow pipeline to process Companies House data-dump into dataframes """
import logging

import pandas as pd
from metaflow import conda_base, step, FlowSpec, Parameter

from breadcrumbs import drop_breadcrumb as talk_to_luigi
from utils import (
    COLUMN_MAPPINGS,
    process_organisations,
    process_address,
    process_sectors,
    process_names,
)

DATA_DUMP_PATH = (
    "s3://nesta-glass/companies_house/BasicCompanyDataAsOneFile-2020-07-01.zip"
)


@conda_base(
    libraries={
        "toolz": "0.11.0",
        "pandas": ">=1.0.0",
        # "smart_open": ">=3.0.0",
        # "fsspec": "<=0.8.4",
        "s3fs": ">=0.4.1",
    }
)
@talk_to_luigi
class CompaniesHouseDump(FlowSpec):
    """ """

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )

    data_dump_path = Parameter(
        "data_dump_path",
        help="Path to Companies House data dump",
        type=str,
        default=DATA_DUMP_PATH,
    )

    @step
    def start(self):
        """ Load raw data """
        logging.info("Loading raw data")

        self.next(self.test)

    @step
    def test(self):
        """ Constrain data to test size if `test` flag is True"""

        if self.test_mode:
            nrows = 10_000
            logging.warning(
                f"TEST MODE: Constraining to first {nrows} orgs or {nrows} rows..."
            )
        else:
            nrows = None

        self.raw = (
            pd.read_csv(
                self.data_dump_path, usecols=COLUMN_MAPPINGS.keys(), nrows=nrows
            )
            .rename(columns=COLUMN_MAPPINGS)
            .drop_duplicates()
        )
        self.next(self.do_organisation)

    @step
    def do_organisation(self):
        """ Process organisations """
        self.organisation = process_organisations(self.raw)
        self.next(self.do_address)

    @step
    def do_address(self):
        """ Process addresses """
        self.address = process_address(self.raw)
        self.next(self.do_sectors)

    @step
    def do_sectors(self):
        """ Process sectors """
        self.sectors = process_sectors(self.raw)
        self.next(self.do_names)

    @step
    def do_names(self):
        """ Process previous and existing company names """
        self.names = process_names(self.raw)
        self.next(self.end)

    @step
    def end(self):
        """ """
        self.table_names = [
            "organisation",
            "address",
            "sectors",
            "names",
        ]
        assert all([hasattr(self, name) for name in self.table_names])


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()],
        level=logging.INFO,
    )
    CompaniesHouseDump()
