import abc
import datetime
import re
from pathlib import Path
from typing import Dict, List, Any, Generator, Optional

import metaflow as mf
import luigi
import pandas as pd
import toolz.curried as t
from numpy import nan
from sqlalchemy_utils.functions import get_declarative_base
from sqlalchemy.sql.expression import func

from daps_utils import CurateTask, MetaflowTask
from daps_utils.db import db_session, insert_data

import research_daps.orms.companies_house as orms


class CHParameters(object):
    """Mixin for parameters common to Glass Tasks."""

    date = luigi.DateParameter()
    data_dump_path = luigi.Parameter(
        description="Metaflow input: Path to Companies house data dump"
    )
    dump_date = luigi.DateParameter()


def parse_date(date_str: str, format: str = "%d/%m/%Y") -> Optional[datetime.date]:
    """Parse a `date_str` as a '%d/%m/%Y' date or return None."""
    try:
        return datetime.datetime.strptime(date_str, format).date()
    except TypeError:
        return None


class CurateDependentOnOtherCurateTask(CurateTask, CHParameters):
    """Requires a `MetaflowTask`, and another `CurateTask`.

    The `CurateTask` dependency should be dependent on the same `MetaFlowTask`.

    `run` changes from `CurateTask.run` to read `self.input()` as list.
    """

    def run(self):
        self.s3path = self.input()[0].open("r").read()
        data = self.curate_data(self.s3path)
        with db_session(database="dev" if self.test else "production") as session:
            # Create the table if it doesn't already exist
            engine = session.get_bind()
            Base = get_declarative_base(self.orm)
            Base.metadata.create_all(engine)
            # Insert the data
            insert_data(data, self.orm, session, low_memory=self.low_memory)
        return self.output().touch()

    def requires(self):
        tag = "dev" if self.test else "production"
        yield MetaflowTask(
            flow_path=self.flow_path,
            flow_tag=tag,
            rebuild_base=self.rebuild_base,
            rebuild_flow=self.rebuild_flow,
            flow_kwargs=self.flow_kwargs,
            preflow_kwargs=self.preflow_kwargs,
            container_kwargs=self.container_kwargs,
            requires_task=self.requires_task,
            requires_task_kwargs=self.requires_task_kwargs,
        )
        yield self.curate_task_dependency(
            # GlassParameters(
            **common_args(self, test=self.test),
            orm=self.curate_orm_dependency,  # orms.Organisation,
        )


class Company(CurateTask, CHParameters):
    def curate_data(self, s3path):
        run_id = Path(s3path).name

        return (
            mf.Run(f"CompaniesHouseDump/{run_id}")
            .data.organisation.rename(
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
            .assign(
                dissolution_date=lambda x: x.dissolution_date.apply(parse_date),
                incorporation_date=lambda x: x.incorporation_date.apply(parse_date),
            )
            .replace({nan: None, pd.NaT: None})
            .to_dict(orient="records")
        )


class CurateDependentOnCHCompany(CurateDependentOnOtherCurateTask):
    curate_task_dependency = Company
    curate_orm_dependency = orms.Company


def remap_sectors(sectors: pd.DataFrame) -> pd.DataFrame:
    """Remap pre-2007 SIC codes to 2007 SIC codes if text matches, otherwise drop."""

    def clean(series: pd.Series) -> pd.Series:
        return series.str.lower().str.strip().str.replace(r"\s+", " ")

    code_text = (
        sectors.SIC5_full.str.split(" - ", expand=True)
        .rename(columns={0: "code", 1: "text"})
        .assign(code_length=lambda x: x.code.str.len())
    )

    choices = (
        code_text.query("code_length == 5")[["code", "text"]]
        .drop_duplicates()
        .assign(text_match=lambda x: x.text.pipe(clean))
    )

    remappings = (
        code_text.reset_index()
        .loc[lambda x: x.code_length == 4, ("text", "index")]
        .assign(text_match=lambda x: x.text.pipe(clean))
        .merge(choices, on="text_match")
        .drop("text_match", axis=1)
        .set_index("index")
    )

    code_text.loc[remappings.index, :] = remappings.assign(code_length=45)
    return (
        sectors.join(code_text)
        .drop(["SIC5_code", "SIC5_full"], 1)
        .rename(columns={"code": "SIC5_code", "text": "SIC5_full"})
        .query("code_length == 5")
        .drop("code_length", axis=1)
    )


def make_sector(df):
    return (
        df[["SIC5_code", "SIC5_full"]]
        .pipe(remap_sectors)
        .drop_duplicates()
        .sort_values("SIC5_code")
        .rename(columns={"SIC5_code": "sector_id", "SIC5_full": "sector_name"})
        # .reset_index(drop=True)  # Start from zero
        # .assign(sector_id=lambda x: x.index + 1)
    )


class CHCompanyName(CurateDependentOnCHCompany):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        return (
            mf.Run(f"CompaniesHouseDump/{run_id}")
            .data.names.rename(
                columns={"company_name": "name", "change_date": "invalid_date"}
            )
            .fillna(self.dump_date.strftime("%d/%m/%Y"))
            .assign(
                invalid_date=lambda x: x.invalid_date.pipe(
                    pd.to_datetime, format="%d/%m/%Y"
                ).apply(lambda y: y.date())
            )
        ).to_dict(orient="records")

    def run(self):
        self.s3path = self.input()[0].open("r").read()
        data = self.curate_data(self.s3path)

        with db_session(database="dev" if self.test else "production") as session:
            # Create the table if it doesn't already exist
            engine = session.get_bind()
            Base = get_declarative_base(self.orm)
            Base.metadata.create_all(engine)

            # Drop all `CompanyName` for companies we are updating
            # This avoids having to carefully update age indexes at the cost of
            # potentially losing some very old name changes
            # (this almost certainly won't happen)
            updating_companies = map(lambda row: row["company_number"], data)
            session.query(orms.CompanyName).filter(
                orms.CompanyName.company_number.in_(updating_companies)
            ).delete(
                synchronize_session=False
            )  # CAREFUL: Session is not synchronised, so don't rely on it!

            # Insert the data
            insert_data(data, self.orm, session, low_memory=self.low_memory)

        return self.output().touch()


class CHSector(CurateDependentOnCHCompany):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        return (
            mf.Run(f"CompaniesHouseDump/{run_id}").data.sectors.pipe(make_sector)
        ).to_dict(orient="records")


class CurateDependentOnCHSector(CurateDependentOnOtherCurateTask):
    curate_task_dependency = CHSector
    curate_orm_dependency = orms.Sector


class CHCompanySector(CurateDependentOnCHSector):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        data = mf.Run(f"CompaniesHouseDump/{run_id}").data

        # sector = data.sectors.pipe(make_sector)

        return (
            data.sectors.pipe(remap_sectors)
            .rename(columns={"SIC5_code": "sector_id", "SIC5_full": "sector_name"})[
                ["company_number", "sector_id", "rank"]
            ]
            .assign(
                date=self.dump_date,
            )
        ).to_dict(orient="records")


def collation(string: str) -> str:
    """Processes `string` ready for comparison with other strings."""
    return string.casefold()[: orms.ADDRESS_TEXT_CHAR_LIM]


def generate_links(
    data: List[Dict[str, Any]],
    existing_addresses: Dict[str, orms.Address],
    init_counter: int,
):
    counter = t.iterate(lambda x: x + 1, init_counter)
    for _ in range(len(data)):
        yield generate_link(data.pop(), existing_addresses, counter)


def generate_link(
    row: Dict[str, Any], existing_addresses: Dict[str, orms.Address], counter: Generator
):
    """Generate `OrganisationAddress`, creating/linking `Address` where necessary."""
    # Get existing address or create and insert new one
    address = existing_addresses.setdefault(
        collation(row["address_text"]),
        # WARNING: mutation propagates out of the function!
        orms.Address(
            address_id=next(counter),
            address_text=row["address_text"],
            postcode=row["postcode"],
        ),
    )
    return orms.CompanyAddress(
        company_number=row["company_number"], date=row["date"], address=address
    )


class AddressCurateTask(CurateDependentOnCHCompany):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        links = (
            mf.Run(f"CompaniesHouseDump/{run_id}")
            .data.address.assign(
                date=self.dump_date,
                address_text=lambda x: x.line1 + ", " + x.line2,
            )[["company_number", "address_text", "postcode", "date"]]
            .dropna()
        )
        print(f"Number of links: {links.shape[0]}")

        return links.to_dict(orient="records")

    def run(self):
        self.s3path = self.input()[0].open("r").read()
        data = self.curate_data(self.s3path)

        with db_session(database="dev" if self.test else "production") as session:
            # Create the table if it doesn't already exist
            engine = session.get_bind()
            Base = get_declarative_base(self.orm)
            Base.metadata.create_all(engine)

            last_date = session.query(func.max(orms.CompanyAddress.date)).scalar()
            # What about if we do an old `dump_date`? Does this affect the counter?
            #       FOR NOW: error if dump date not later than latest existing in DB
            assert (not last_date) or (
                self.dump_date >= last_date
            ), "Can't add an old dump safely!"

            # Drop all `CompanyAddress` for current `dump_date`
            session.query(orms.CompanyAddress).filter(
                orms.CompanyAddress.date == self.dump_date
            ).delete()

            init_counter = (
                session.query(func.max(orms.Address.address_id)).scalar() or 0
            ) + 1

            existing_addresses: Dict[str] = {
                collation(addr.address_text): addr
                for addr in session.query(orms.Address).all()
            }  # Warning: `generate_links` mutates this

            links = generate_links(data, existing_addresses, init_counter)

            session.add_all(links)

        return self.output().touch()


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)
    # dump_date = luigi.Parameter(description="Metaflow input: format 'DD/MM/YYYY'")
    data_dump_path = luigi.Parameter(
        description="Metaflow input: Path to Companies house data dump"
    )

    def requires(self):
        tasks = [
            (Company, orms.Company),
            (CHCompanySector, orms.CompanySector),
            (CHCompanyName, orms.CompanyName),
            (AddressCurateTask, orms.CompanyAddress),
        ]

        for Task_, Orm_ in tasks:
            yield Task_(
                **common_args(self, test=not self.production),
                orm=Orm_,
            )


def common_args(self: object, test: bool) -> dict:
    """Common args for Glass Tasks."""
    return dict(
        date=self.date,
        dump_date=parse_date(
            re.findall(r"\d{4}-\d{2}-\d{2}", self.data_dump_path)[0], format="%Y-%m-%d"
        ),
        flow_path="companies_house/companies_house_dump.py",
        flow_kwargs={
            "data_dump_path": self.data_dump_path,
            "test_mode": test,
        },
        preflow_kwargs={
            "datastore": "s3",
            "package-suffixes": ".txt",
            "environment": "conda",
        },
        test=test,
        data_dump_path=self.data_dump_path,
    )
