"""Temporary solution to merge Companies House data
until a luigi task is written.

Scalability of this is such a problem (~50M records) that with this temporary
 solution, we'll just stick to a few months of data (may, jun, jul) for which
 we have Glass data.
"""

import datetime
import re

import pandas as pd
from typing import Iterable, List
import toolz.curried as t
from metaflow import FlowSpec, Parameter, step, JSONType, Flow, Run, conda_base


def get_flow_params(param: str, flows: List[Flow]) -> Iterable:
    return t.pipe(
        flows,
        t.map(lambda x: getattr(x.data, param)),
        # list,
    )


def all_equal(xs: List) -> bool:
    return len(set(xs)) <= 1


def all_different(xs: List) -> bool:
    return len(set(xs)) == len(xs)


def return_if_equal(left, right):
    pd.testing.assert_frame_equal(left, right)
    return left


DATE_REGEX = r"BasicCompanyDataAsOneFile-([0-9]{4}-[0-9]{2}-[0-9]{2})"
DATE_STRPTIME_FORMAT = "%Y-%m-%d"


def get_dates_from_dump_path(dump_paths: Iterable[str]) -> List[datetime.date]:
    return [
        datetime.datetime.strptime(
            re.findall(DATE_REGEX, s)[0], DATE_STRPTIME_FORMAT
        ).date()
        for s in dump_paths
    ]


@conda_base(
    libraries={
        "toolz": "0.11.0",
        "pandas": ">=1.1.0",
    }
)
class CompaniesHouseMergeDumpFlow(FlowSpec):
    """Merge Companies House data dumps together"""

    flow_ids = Parameter(
        "flow_ids",
        help="List of flow ID's of Companies House data-dumps to merge",
        type=JSONType,
        required=True,
    )

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )

    def get_flows(self, flow_ids: List[int]) -> List[Flow]:
        return list(map(lambda flow_id: Run(f"CompaniesHouseDump/{flow_id}"), flow_ids))

    @step
    def start(self):

        flows = self.get_flows(self.flow_ids)

        assert all_equal(
            [self.test_mode, *get_flow_params("test_mode", flows)]
        ), "Flow ID's do not match test_mode"

        dump_paths = get_flow_params("data_dump_path", flows)
        self.dates = get_dates_from_dump_path(dump_paths)
        assert all_different(self.dates), "Flow dates are not distinct"

        assert self.dates == sorted(
            self.dates
        ), "Please order flows by date (earliest first)"

        self.next(self.merge_sector)

    @step
    def end(self):
        pass

    @step
    def merge_sector(self):
        def process_sector(sector: pd.DataFrame) -> pd.DataFrame:
            """Output corresponds to Sector ORM"""
            return (
                sector[["SIC5_code", "SIC5_full"]]
                .pipe(remap_sectors)
                .drop_duplicates()
                .sort_values("SIC5_code")
                .rename(columns={"SIC5_code": "sector_id", "SIC5_full": "sector_name"})
                .reset_index(drop=True)  # Start from zero
                # .assign(sector_id=lambda x: x.index + 1)
            )

        flows = self.get_flows(self.flow_ids)

        self.sector = t.thread_last(
            flows,
            (get_flow_params, "sectors"),
            t.map(process_sector),
            t.reduce(return_if_equal),  # Check they are all the same
        )

        self.next(self.merge_organisation)

    @step
    def merge_organisation(self):
        def process_organisation(organisation: pd.DataFrame) -> pd.DataFrame:
            return organisation.rename(
                columns={"company_category": "category", "company_status": "status"}
            )[
                [
                    "company_number",
                    "category",
                    "status",
                    "country_of_origin",
                    "dissolution_date",
                    "incorporation_date",
                    "uri",
                ]
            ]

        flows = self.get_flows(self.flow_ids)

        self.organisation = t.thread_last(
            flows,
            (get_flow_params, "organisation"),
            t.map(process_organisation),
            # Update latest with any old companies not in more recent updates
            t.reduce(
                lambda acc, x: acc.append(x).drop_duplicates(
                    subset=["company_number"], keep="first"
                )
            ),
        )

        self.next(self.merge_organisationsector)

    @step
    def merge_organisationsector(self):
        def process_organisationsector(
            sector: pd.DataFrame,
            date: datetime.datetime,
        ) -> pd.DataFrame:
            return (
                sector.pipe(remap_sectors)
                .rename(columns={"SIC5_code": "sector_id"})[
                    ["company_number", "sector_id", "rank"]
                ]
                .assign(date=date)
            )

        flows = self.get_flows(self.flow_ids)
        self.organisationsector = t.thread_last(
            flows,
            lambda x: zip(get_flow_params("sectors", x), self.dates),
            t.map(lambda x: process_organisationsector(*x)),
            pd.concat,
        )

        self.next(self.merge_organisationname)

    @step
    def merge_organisationname(self):
        def process_organisationname(names: pd.DataFrame) -> pd.DataFrame:
            return names.rename(
                columns={"company_name": "name", "change_date": "invalid_date"}
            )

        flows = self.get_flows(self.flow_ids)
        self.organisationname = t.thread_last(
            flows,
            (get_flow_params, "names"),
            t.map(process_organisationname),
            # Update latest with any old companies not in more recent updates
            t.reduce(
                lambda acc, x: acc.append(x).drop_duplicates(
                    subset=["company_number", "name_age_index"], keep="first"
                )
            ),
        )
        self.next(self.merge_address)

    @step
    def merge_address(self):
        def process_address(address: pd.DataFrame) -> pd.DataFrame:
            return address.assign(address_text=lambda x: x.line1 + ", " + x.line2)[
                ["address_text", "postcode"]
            ]

        flows = self.get_flows(self.flow_ids)
        self.address = (
            t.thread_last(
                flows,
                reversed,  # Older addresses need lower id's
                (get_flow_params, "address"),
                t.map(process_address),
                pd.concat,
            )
            .drop_duplicates(subset=["address_text"])
            .reset_index(drop=True)
            .assign(address_id=lambda x: x.index + 1)
        )
        # self.next(self.end)
        self.next(self.merge_organisationaddress)

    @step
    def merge_organisationaddress(self):
        def process_organisationaddress(
            address: pd.DataFrame, date: datetime.datetime, address_lookup: pd.DataFrame
        ) -> pd.DataFrame:
            return address.assign(
                address_text=lambda x: x.line1 + ", " + x.line2, date=date
            ).merge(address_lookup, on="address_text")[
                ["company_number", "address_id", "date"]
            ]

        flows = self.get_flows(self.flow_ids)
        self.organisationaddress = t.thread_last(
            flows,
            # reversed,  # Older addresses need lower id's
            lambda x: zip(get_flow_params("address", x), self.dates),
            t.map(lambda x: process_organisationaddress(*x, self.address)),
            pd.concat,
        )
        self.next(self.end)


def remap_sectors(sectors: pd.DataFrame) -> pd.DataFrame:
    """Remap pre-2007 SIC codes to 2007 SIC codes if text matches, otherwise drop."""

    def clean(series: pd.Series) -> pd.Series:
        return series.str.lower().str.strip().str.replace(r"\s+", " ")

    code_text = (
        sectors.SIC5_full.str.split(" - ", expand=True)
        .rename(columns={0: "code", 1: "text"})
        .assign(code_length=lambda x: x.code.str.len())
    )

    # 5-digit SIC code and description combinations
    choices = (
        code_text.query("code_length == 5")[["code", "text"]]
        .drop_duplicates()
        .assign(text_match=lambda x: x.text.pipe(clean))
    )

    # 4-digit SIC codes remapped to 5-digit SIC codes based on matching text
    remappings = (
        code_text.reset_index()
        .loc[lambda x: x.code_length == 4, ("text", "index")]
        .assign(text_match=lambda x: x.text.pipe(clean))
        .drop("text", axis=1)
        .merge(choices, on="text_match")
        .drop("text_match", axis=1)
        .set_index("index")
    )

    # Update `code_text` w/ remapped 4-digit SIC codes and join back to `sectors`
    code_text.loc[remappings.index, :] = remappings.assign(code_length=5)
    return (
        sectors.join(code_text)
        .drop(["SIC5_code", "SIC5_full"], 1)
        .rename(columns={"code": "SIC5_code", "text": "SIC5_full"})
        .query("code_length == 5")
        .drop("code_length", axis=1)
    )


if __name__ == "__main__":
    CompaniesHouseMergeDumpFlow()
