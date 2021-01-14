""" Metaflow pipeline to process raw Glass AI notices into dataframes """
import datetime
import logging

# import subprocess
# import sys
# from pathlib import Path

from metaflow import FlowSpec, Parameter, conda_base, step

from breadcrumbs import drop_breadcrumb as talk_to_luigi

# path = Path(__file__).resolve().parent / "requirements.txt"
# output = subprocess.run(
#     [sys.executable, "-m", "pip", "install", "-r", path], capture_output=True
# )
# print(output.stdout)
# print(output.stderr)


@conda_base(
    libraries={
        "toolz": "==0.11.0",
        "pandas": ">=1.0.0",
        "bulwark": ">=0.6.1",
        "s3fs": "==0.5.1",
    }
)
@talk_to_luigi
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
        type=lambda x: datetime.datetime.strptime(x, "%m/%Y"),
    )

    @step
    def start(self):
        """ Load raw data """
        from process_raw import glass_df_loader

        logging.info("Loading raw data")

        if self.test_mode:
            nrows = 10_000
            logging.warning(f"TEST MODE: Constraining to first {nrows} notices...")
        else:
            nrows = None

        self.notices = glass_df_loader(self.s3_input, nrows=nrows)

        self.next(self.process)

    @step
    def process(self):
        """ Process tables """
        from process_raw import process_notice

        logging.info("Processing Notice")
        self.notices, self.term = process_notice(self.notices, self.date)
        self.next(self.end)

    @step
    def end(self):
        """ """
        pass


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()],
        level=logging.INFO,
    )

    GlassNoticesDumpFlow()
