import datetime
from pathlib import Path
from typing import Dict, List, Any, Generator, Tuple

import metaflow as mf
import luigi
import toolz.curried as t
from sqlalchemy_utils.functions import get_declarative_base
from sqlalchemy.sql.expression import func

from daps_utils import CurateTask

# from daps_utils.parameters import SqlAlchemyParameter
from daps_utils.db import db_session, insert_data

import research_daps.orms.glass as orms


class GlassParameters(object):
    """Mixin for parameters common to Glass Tasks."""

    date = luigi.DateParameter()
    dump_date = luigi.Parameter()
    s3_input = luigi.Parameter()


def generate_links(
    data: Tuple[List[Dict[str, Any]], Dict[str, List[str]]],
    existing_terms: Dict[str, orms.CovidTerm],
    init_counter: int,
):
    notices, terms = data
    counter = t.iterate(lambda x: x + 1, init_counter)
    for _ in range(len(notices)):
        row = notices.pop()
        row_terms = terms[row["notice_id"]]
        yield generate_link(row, row_terms, existing_terms, counter)


def generate_link(
    row: Dict[str, Any],
    row_terms: List[str],
    existing_terms: Dict[str, orms.CovidTerm],
    counter: Generator,
):
    """Generate ``, creating/linking `` where necessary."""
    # Get existing address or create and insert new one
    terms = [
        existing_terms.setdefault(
            collation(term),
            # WARNING: mutation propagates out of the function!
            orms.CovidTerm(term_id=next(counter), term_string=term, date=row["date"]),
        )
        for term in row_terms
    ]

    return orms.Notice(
        notice_id=row["notice_id"],
        org_id=row["org_id"],
        date=row["date"],
        snippet=row["snippet"],
        url=row["url"],
        terms=terms,
    )


def collation(string: str) -> str:
    """Processes `string` ready for comparison with other strings."""
    return string.casefold()[: orms.ADDRESS_TEXT_CHAR_LIM]


class TermCurateTask(CurateTask, GlassParameters):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        data = mf.Run(f"GlassNoticesDumpFlow/{run_id}").data

        # Terms is a lookup: notice_id -> List of terms
        return (
            data.notices.rename(columns={"id_organisation": "org_id"})
            .assign(
                date=lambda x: datetime.datetime.strptime(self.dump_date, "%m/%Y").date()
            )
            .to_dict(orient="records"),
            data.term.groupby("notice_id").matched_terms.unique().astype(str).to_dict(),
        )

    def run(self):
        self.s3path = self.input().open("r").read()
        data: tuple = self.curate_data(self.s3path)
        with db_session(database="dev" if self.test else "production") as session:
            # Create the table if it doesn't already exist
            engine = session.get_bind()
            Base = get_declarative_base(self.orm)
            Base.metadata.create_all(engine)

            # # Drop all `Notice` for current `dump_date`
            # session.query(orms.Notice).filter(
            #     orms.Notice.date
            #     == datetime.datetime.strptime(self.dump_date, "%m/%Y").date()
            # ).delete()

            init_counter = (
                session.query(func.max(orms.CovidTerm.term_id)).scalar() or 0
            ) + 1

            existing_terms: Dict[str] = {
                collation(addr.term_string): addr
                for addr in session.query(orms.CovidTerm).all()
            }  # Warning: `generate_links` mutates this

            links = generate_links(data, existing_terms, init_counter)

            session.add_all(links)

        return self.output().touch()


def make_sector(df):
    return (
        df[["sector_name"]]
        .drop_duplicates()
        .sort_values("sector_name")
        .reset_index(drop=True)  # Start from zero
        .assign(sector_id=lambda x: x.index + 1)
    )


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)
    dump_date = luigi.Parameter(description="Metaflow input: format 'MM/YYYY'")
    s3_input = luigi.Parameter()

    def requires(self):
        return TermCurateTask(
            **common_args(self, test=not self.production),
            orm=orms.Notice,
        )


def common_args(self: Any, test: bool) -> dict:
    """Common args for Glass Tasks."""
    return dict(
        date=self.date,
        dump_date=self.dump_date,
        flow_path="glass_ai/notices.py",
        flow_kwargs={
            "s3_input": self.s3_input,
            "test_mode": test,
            "date": self.dump_date,
        },
        preflow_kwargs={
            "datastore": "s3",
            "package-suffixes": ".txt",
            "environment": "conda",
        },
        test=test,
        s3_input=self.s3_input,
    )
