""" Metaflow pipeline to process raw Glass AI data into dataframes """
import logging
import datetime
import pandas as pd

from process_raw import (
    glass_df_loader,
    process_address,
    process_organisations,
    check_organisations,
    process_sectors,
)
from metaflow import FlowSpec, Parameter, step, JSONType


req_keys = ["organisation", "address", "sector"]


class GlassMainDumpFlow(FlowSpec):
    """Clean and process raw Glass AI data into dataframes"""

    table_names = [
            "address",
            "sectors",
            "organisation",
            "glass_company_match",
        ]

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )

    s3_inputs = Parameter(
        "s3_inputs",
        help=f"S3 locations for data. Requires JSON keys: {req_keys}",
        type=JSONType,
        required=True,
    )

    date = Parameter(
        "date",
        help="Datetime of data-dump. MM/YYYY",
        type=lambda x: datetime.datetime.strptime(x, '%m/%Y'),
        required=True,
    )

    @step
    def start(self):
        """ Load raw data """
        logging.info("Loading raw data")

        if not all((k in self.s3_inputs.keys() for k in req_keys)):
            raise ValueError(f"""`s3_inputs` requires keys: {req_keys}
                             got: {list(self.s3_input.keys())}
                             """)

        if self.test_mode:
            nrows = 10_000
            logging.warning(
                f"TEST MODE: Constraining to first {nrows} orgs or {nrows} rows."
            )
            test_ids = glass_df_loader(  # First `nrows` organisation ids
                self.s3_inputs["organisation"], nrows=nrows
            ).id_organisation.values

            def test_filter(df: pd.DataFrame) -> pd.DataFrame:
                """Load limited size dataframe for test mode"""
                if 'id_organisation' not in df.columns:
                    return df
                return df.loc[lambda x: x.id_organisation.isin(test_ids)]
        else:
            def test_filter(x: pd.DataFrame) -> pd.DataFrame:
                return x

        self.organisation = glass_df_loader(self.s3_inputs["organisation"]).pipe(test_filter)
        self.address = glass_df_loader(self.s3_inputs["address"]).pipe(test_filter)
        self.sector = glass_df_loader(self.s3_inputs["sector"]).pipe(test_filter)

        self.next(self.process)

    @step
    def process(self):
        """ Process tables """

        logging.info("Processing Address")
        self.address = process_address(self.address)
        n_address = self.address.groupby("id_organisation").size().rename("n_address")

        # logging.info("Processing Notice")
        # self.covid_notices_may2020 = process_notice(self.covid_notices_may2020)
        # self.covid_notices_june2020 = process_notice(self.covid_notices_june2020)
        # has_covid_notice = (
        #     pd.concat(
        #         [
        #             self.covid_notices_may2020.id_organisation,
        #             self.covid_notices_june2020.id_organisation,
        #         ]
        #     )
        #     .to_frame()
        #     .drop_duplicates()
        #     .set_index("id_organisation")
        #     .assign(has_covid_notice=True)
        #     .has_covid_notice
        # )

        logging.info("Processing Sectors")
        self.sector = process_sectors(self.sector)
        n_sectors = self.sector.groupby("id_organisation").size().rename("n_sectors")

        logging.info("Processing Organisation")
        organisation = process_organisations(
            self.organisation,
        )
        low_quality = check_organisations(
            organisation,
            n_address,
            n_sectors,
        )

        # Split off glass' in-house companies house match
        ch_match_cols = ["company_number", "company_number_match_type"]
        self.organisation = organisation.drop(ch_match_cols, axis=1).join(low_quality).assign(date=self.date)
        self.glass_company_match = organisation[["id_organisation", *ch_match_cols]]

        self.next(self.end)

    @step
    def end(self):
        """ """
        pass


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()], level=logging.INFO,
    )

    GlassMainDumpFlow()
