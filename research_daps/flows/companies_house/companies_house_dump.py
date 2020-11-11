""" Metaflow pipeline to process Companies House data-dump into dataframes """
import logging

import pandas as pd

import research_daps
from utils import (
    COLUMN_MAPPINGS,
    process_organisations,
    process_address,
    process_sectors,
    process_names,
)
from metaflow import FlowSpec, Parameter, step

DATA_DUMP_PATH = (
    "s3://nesta-glass/companies_house/BasicCompanyDataAsOneFile-2020-07-01.zip"
)
DATA_DUMP_PATH = (
    "/media/s3fs/companies_house/BasicCompanyDataAsOneFile-2020-07-01.zip"
)
# NSPL_PATH = f"{research_daps.project_dir}/data/raw/nspl/Data/NSPL_NOV_2019_UK.csv"


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
    # nspl_path = Parameter(
    #     "nspl_path", help="Path to NSPL data", type=str, default=NSPL_PATH
    # )

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
        # self.next(self.nspl)

        # @step
        # def nspl(self):
        #     """ Load NSPL auxilliary dataset """

        #     nspl_cols = [
        #         "pcds",
        #         "laua",
        #         "lat",
        #         "long",
        #     ]
        #     self._nspl = pd.read_csv(self.nspl_path, usecols=nspl_cols).rename(
        #         columns={"pcds": "postcode"}
        #     )
        self.next(self.do_organisation)

    @step
    def do_organisation(self):
        """ Process organisations """
        self.organisation = process_organisations(self.raw)
        self.next(self.do_address)

    @step
    def do_address(self):
        """ Process addresses """
        self.address = process_address(self.raw)  # , self._nspl)
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
