"""Temporary solution to merge Glass AI notice data
until a luigi task is written
"""

import pandas as pd
from typing import List
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


class GlassMergeNoticesDumpFlow(FlowSpec):
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
            map(lambda flow_id: Run(f"GlassNoticesDumpFlow/{flow_id}"), self.flow_ids)
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
        self.next(self.merge_term)

    @step
    def merge_term(self):
        self.term = t.thread_last(
            self.flows, (get_flow_params, "term"), pd.concat
        ).dropna().drop("low_quality", axis=1)

        self.next(self.merge_notice)

    @step
    def merge_notice(self):
        self.notices = (
            t.thread_last(
                self.flows,
                (get_flow_params, "notices"),
                pd.concat
            )
            .dropna()
            .drop("low_quality", axis=1)
        )

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    GlassMergeNoticesDumpFlow()
