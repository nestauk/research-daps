"""Metaflow pipeline to fetch National Statistics Postcode Lookup."""
import pandas as pd
from metaflow import (
    conda_base,
    step,
    FlowSpec,
    Parameter,
)

from nspl_utils import (
    chrome_driver,
    find_download_url,
    download_zip,
    parse_geoportal_url_to_nspl_csv,
)

GEOPORTAL_URL_PREFIX = "https://geoportal.statistics.gov.uk/datasets"


@conda_base(
    libraries={
        "beautifulsoup4": "4.9.3",
        "cytoolz": "0.11.0",
        "lxml": "4.5.1",
        "pandas": "1.1.0",
        "selenium": "3.141.0",
        "toolz": "0.11.0",
    }
)
class NSPL(FlowSpec):
    geoportal_url = Parameter(
        "geoportal-url",
        help=f"{GEOPORTAL_URL_PREFIX}/<geo-portal-url>",
        required=True,
        type=str,
        default=f"{GEOPORTAL_URL_PREFIX}/national-statistics-postcode-lookup-november-2020",
    )

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode",
        type=bool,
        default=True,
    )

    @step
    def start(self):
        """Get dynamically rendered download link, download, and filter columns."""

        # Get download location
        with chrome_driver() as driver:
            download_url = find_download_url(driver, self.geoportal_url)

        # Download
        zipfile = download_zip(download_url)

        # Filter/load
        csv_filename = parse_geoportal_url_to_nspl_csv(self.geoportal_url)
        keep_columns = ["pcds", "laua", "nuts", "lat", "long", "rgn", "ttwa"]
        nrows = 1_000 if self.test_mode else None
        self.nspl_data = pd.read_csv(
            zipfile.open(f"Data/{csv_filename}"), nrows=nrows, usecols=keep_columns
        )

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NSPL()
