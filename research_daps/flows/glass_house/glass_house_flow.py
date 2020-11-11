""" Metaflow pipeline to process Companies House data-dump into dataframes """
import logging

import research_daps
from research_daps.flows.glass_ai.ingest_data_dump import GlassMainDumpFlow
from research_daps.flows.companies_house.companies_house_dump import CompaniesHouseDump
from metaflow import Parameter, step, current, get_metadata

from cytoolz.curried import curry, pipe

from jacc_hammer.fuzzy_hash import (
    Cos_config,
    Fuzzy_config,
    match_names_stream,
    stream_sim_chunks_to_hdf,
)
from jacc_hammer.name_clean import preproc_names
from jacc_hammer.top_matches import get_top_matches_chunked
from research_daps import flow_getter
import boto3


def download_file(path):
    """ Download `data/{path}` from `BUCKET`

    Args:
        path (str): Path to file after `data/` in `project_dir`
            E.g. `path="processed/model_predictions.csv"` will
            fetch the s3 key
            `"s3://{BUCKET}/data/processed/model_prections.csv"`
            to `{project_dir}/data/processed/model_predicions.csv`
    """

    object_path = "data/" + path

    s3 = boto3.client("s3")
    s3.download_file("nesta-glass", object_path, path)


class GlassHouseMatch(CompaniesHouseDump, GlassMainDumpFlow):
    """ Match glass to Companies House """

    # TODO: Understand the best way to consistently chain/combine flows
    companies_house_flow_id = Parameter(
        "compaies_house_flow_id",
        help="Metaflow run ID to provide CH data",
        default=None,
        type=int,
    )
    glass_flow_id = Parameter(
        "glass_flow_id",
        help="Metaflow run ID to provide glass data",
        default=None,
        type=int,
    )

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )

    @step
    def start(self):
        """ Load raw data """

        # TODO check both test_mode parameters agree
        logging.info(f"Test? {self.test_mode}")

        # Interim results alongside other run data in `.metaflow`
        # self.tmp_dir = "/".join(
        #     [get_metadata(), ".metaflow", current.flow_name, current.run_id]
        # ).strip("local@")

        self.tmp_dir = "/tmp"

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

        logging.info(f"{self.glass_flow_id}")
        glass = flow_getter("GlassMainDumpFlow", run_id=self.glass_flow_id)
        logging.info(f"{self.companies_house_flow_id}")
        ch = flow_getter("CompaniesHouseDump", run_id=self.companies_house_flow_id)
        self.names_x = (
            glass("organisation")[["org_id", "organisation_name"]]
            .head(nrows)
            .set_index("org_id")
            .organisation_name
        )
        self.names_y = (
            ch("names")[["company_number", "company_name"]]
            .head(nrows)
            .set_index("company_number")
            .company_name
        )

        self.next(self.process_names)

    @step
    def process_names(self):
        """ Pre-process names """
        self.names = [
            name.pipe(preproc_names).dropna()
            for i, name in enumerate([self.names_x, self.names_y])
        ]
        self.next(self.match)

    @step
    def match(self):
        """ The core fuzzy matching algorithm """
        cos_config = Cos_config()
        fuzzy_config = Fuzzy_config(num_perm=128)
        match_config = dict(
            threshold=33,
            chunksize=100,
            cos_config=cos_config,
            fuzzy_config=fuzzy_config,
            tmp_dir=self.tmp_dir,
        )
        self.f_fuzzy_similarities = f"{self.tmp_dir}/fuzzy_similarities"
        out = pipe(
            list(map(lambda x: x.values, self.names)),
            curry(match_names_stream, **match_config),
            curry(stream_sim_chunks_to_hdf, fout=self.f_fuzzy_similarities),
        )
        assert out == self.f_fuzzy_similarities, out
        self.next(self.find_top_matches)

    @step
    def find_top_matches(self):
        """ Find the top matches for each organisation """
        chunksize = 1e7

        self.top_matches = get_top_matches_chunked(
            self.f_fuzzy_similarities, chunksize=chunksize, tmp_dir=self.tmp_dir
        )

        self.next(self.end)

    @step
    def end(self):
        """ """
        self.company_numbers = (
            self.top_matches.merge(
                self.names_y.reset_index(),
                left_on="y",
                right_index=True,
                validate="1:1",
            )
            .merge(
                self.names_x.reset_index(),
                left_on="x",
                right_index=True,
                validate="m:1",
            )
            .drop(["x", "y"], axis=1)
        )


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()], level=logging.INFO,
    )
    GlassHouseMatch()

