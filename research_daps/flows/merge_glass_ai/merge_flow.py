"""Temporary solution to merge Glass AI data
until a luigi task is written
"""

import datetime
import pandas as pd
from typing import List
from operator import eq
from functools import reduce
import toolz.curried as t
from metaflow import FlowSpec, Parameter, step, JSONType, Flow, Run


def get_flow_params(param: str, flows: List[Flow]) -> List:
    return t.pipe(
        flows,
        t.map(lambda x: getattr(x.data, param)),
        list,
    )


def all_equal(xs: List) -> bool:
    return len(set(xs)) <= 1


def all_different(xs: List) -> bool:
    return len(set(xs)) == len(xs)


class GlassMergeMainDumpFlow(FlowSpec):
    """Merge Glass data dumps together"""

    flow_ids = Parameter(
        "flow_ids",
        help="List of flow ID's of glass data-dumps to merge",
        type=JSONType,
        required=True,
    )

    test_mode = Parameter(
        "test_mode",
        help="Whether to run in test mode (on a small subset of data)",
        type=bool,
        default=True,
    )

    @step
    def start(self):

        flows = list(
            map(lambda flow_id: Run(f"GlassMainDumpFlow/{flow_id}"), self.flow_ids)
        )

        assert all_equal(
            [self.test_mode, *get_flow_params("test_mode", flows)]
        ), "Flow ID's do not match test_mode"

        assert all_different(
            get_flow_params("date", flows)
        ), "Flow dates are not distinct"

        # TODO: sort in date order (earliest first)
        assert sorted(
            get_flow_params("date", flows)
        ), "Please order flows by date (earliest first)"

        self.flows = flows
        self.next(self.merge_sector)

    @step
    def merge_sector(self):
        def process_sector(sector: pd.DataFrame) -> pd.DataFrame:
            """Output corresponds to Sector ORM"""
            return (
                sector[["sector_name"]]
                .drop_duplicates()
                .sort_values("sector_name")
                .reset_index(drop=True)  # Start from zero
                .assign(sector_id=lambda x: x.index + 1)
            )

        self.sector = t.thread_last(
            self.flows,
            (get_flow_params, "sector"),
            t.map(process_sector),
            list,
            t.do(lambda x: pd.testing.assert_frame_equal(*x)),
            t.first,
        )

        self.next(self.merge_organisation)

    @step
    def merge_organisation(self):
        def process_organisation(organisation: pd.DataFrame) -> pd.DataFrame:
            return organisation.rename(
                columns={"id_organisation": "org_id", "organisation_name": "name"}
            ).assign(active=False)[["org_id", "name", "website", "active"]]

        self.organisation = t.thread_last(
            self.flows,
            (get_flow_params, "organisation"),
            t.map(process_organisation),
            lambda x: pd.concat([t.first(x).assign(active=True), *x]),
        ).drop_duplicates(subset=["org_id", "name", "website"])

        self.next(self.merge_organisationsector)

    @step
    def merge_organisationsector(self):
        def process_organisationsector(
            sector_: pd.DataFrame, date: datetime.datetime, sector: pd.DataFrame
        ) -> pd.DataFrame:
            return (
                sector_.rename(
                    columns={"id_organisation": "org_id", "sector_rank": "rank"}
                )
                .merge(sector, on="sector_name")
                .assign(date=date)
                .drop("sector_name", axis=1)[["org_id", "sector_id", "date", "rank"]]
            )

        self.organisationsector = t.thread_last(
            self.flows,
            lambda x: zip(get_flow_params("sector", x), get_flow_params("date", x)),
            t.map(lambda x: process_organisationsector(*x, self.sector)),
            pd.concat,
        )

        self.next(self.merge_organisationmetadata)

    @step
    def merge_organisationmetadata(self):
        def process_organisationmetadata(
            organisation: pd.DataFrame, date: datetime.datetime
        ) -> pd.DataFrame:
            return organisation.rename(columns={"id_organisation": "org_id"}).assign(
                date=date
            )[["org_id", "date", "has_webshop", "vat_number", "low_quality"]]

        self.organisationmetadata = t.thread_last(
            self.flows,
            lambda x: zip(
                get_flow_params("organisation", x), get_flow_params("date", x)
            ),
            t.map(lambda x: process_organisationmetadata(*x)),
            pd.concat,
        )
        self.next(self.merge_organisationdescription)

    @step
    def merge_organisationdescription(self):
        def process_organisationdescription(
            organisation: pd.DataFrame, date: datetime.datetime
        ) -> pd.DataFrame:
            return (
                organisation.dropna(subset=["description"])
                .rename(columns={"id_organisation": "org_id"})
                .assign(date=date)[["org_id", "date", "description"]]
            )

        self.organisationdescription = t.thread_last(
            self.flows,
            lambda x: zip(
                get_flow_params("organisation", x), get_flow_params("date", x)
            ),
            t.map(lambda x: process_organisationdescription(*x)),
            pd.concat,
        )
        self.next(self.merge_address)

    @step
    def merge_address(self):
        def process_address(address: pd.DataFrame) -> pd.DataFrame:
            return address[["address_text", "postcode"]]

        self.address = (
            t.thread_last(
                self.flows,
                reversed,  # Older addresses need lower id's
                (get_flow_params, "address"),
                t.map(process_address),
                pd.concat,
            )
            .drop_duplicates(subset=["address_text"])
            .reset_index(drop=True)
            .assign(address_id=lambda x: x.index + 1)
        )
        self.next(self.merge_organisationaddress)

    @step
    def merge_organisationaddress(self):
        def process_organisationaddress(
            address: pd.DataFrame, date: datetime.datetime, address_lookup: pd.DataFrame
        ) -> pd.DataFrame:
            return (
                address.rename(
                    columns={"id_organisation": "org_id", "address_rank": "rank"}
                )
                .assign(date=date)
                .merge(address_lookup, on="address_text")[
                    ["org_id", "address_id", "rank", "date"]
                ]
            )

        self.organisationaddress = t.thread_last(
            self.flows,
            # reversed,  # Older addresses need lower id's
            lambda x: zip(get_flow_params("address", x), get_flow_params("date", x)),
            t.map(lambda x: process_organisationaddress(*x, self.address)),
            pd.concat,
        )
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    GlassMergeMainDumpFlow()
