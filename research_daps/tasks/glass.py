import abc
import datetime
from pathlib import Path
from typing import Dict, List, Any, Generator

import metaflow as mf
import luigi
import toolz.curried as t
from numpy import nan
from sqlalchemy_utils.functions import get_declarative_base
from sqlalchemy.sql.expression import func

from daps_utils import CurateTask, MetaflowTask

# from daps_utils.parameters import SqlAlchemyParameter
from daps_utils.db import db_session, insert_data

import research_daps.orms.glass as orms


class GlassParameters(object):
    """Mixin for parameters common to Glass Tasks."""

    date = luigi.DateParameter()
    dump_date = luigi.Parameter()
    s3_inputs = luigi.Parameter()


class CurateDependentOnOtherCurateTask(CurateTask, GlassParameters):
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


class GlassOrganisation(CurateTask, GlassParameters):
    def curate_data(self, s3path):
        # Get glass flow data
        run_id = Path(s3path).name
        data = mf.Run(f"GlassMainDumpFlow/{run_id}").data.organisation

        # Output as dict?
        return (
            data.rename(
                columns={"id_organisation": "org_id", "organisation_name": "name"}
            )
            .assign(active=True)[["org_id", "name", "website", "active"]]
            .to_dict(orient="records")
        )


class CurateDependentOnGlassOrganisation(CurateDependentOnOtherCurateTask):
    curate_task_dependency = GlassOrganisation
    curate_orm_dependency = orms.Organisation


class GlassOrganisationMetadata(CurateDependentOnGlassOrganisation):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        return (
            mf.Run(f"GlassMainDumpFlow/{run_id}")
            .data.organisation.rename(columns={"id_organisation": "org_id"})
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
                low_quality=lambda x: x.low_quality.fillna(False),
                vat_number=lambda x: x.vat_number.replace({nan: None}),
            )[["org_id", "date", "has_webshop", "vat_number", "low_quality"]]
            .to_dict(orient="records")
        )


class GlassOrganisationDescription(CurateDependentOnGlassOrganisation):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        return (
            mf.Run(f"GlassMainDumpFlow/{run_id}")
            .data.organisation.rename(columns={"id_organisation": "org_id"})
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
            )[["org_id", "date", "description"]]
            .dropna()
        ).to_dict(orient="records")


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
    return orms.OrganisationAddress(
        org_id=row["org_id"], date=row["date"], rank=row["rank"], address=address
    )


def collation(string: str) -> str:
    """Processes `string` ready for comparison with other strings."""
    return string.casefold()[: orms.ADDRESS_TEXT_CHAR_LIM]


class AddressCurateTask(CurateDependentOnGlassOrganisation):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        links = (
            mf.Run(f"GlassMainDumpFlow/{run_id}")
            .data.address.rename(
                columns={
                    "id_organisation": "org_id",
                    "address_rank": "rank",
                }
            )
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
            )
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

            # Drop all `OrganisationAddress` for current `dump_date`
            session.query(orms.OrganisationAddress).filter(
                orms.OrganisationAddress.date
                == datetime.datetime.strptime(self.dump_date, "%m/%Y").date()
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


def make_sector(df):
    return (
        df[["sector_name"]]
        .drop_duplicates()
        .sort_values("sector_name")
        .reset_index(drop=True)  # Start from zero
        .assign(sector_id=lambda x: x.index + 1)
    )


class GlassSector(CurateDependentOnGlassOrganisation):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        return (
            mf.Run(f"GlassMainDumpFlow/{run_id}").data.sector.pipe(make_sector)
        ).to_dict(orient="records")


class CurateDependentOnGlassSector(CurateDependentOnOtherCurateTask):
    curate_task_dependency = GlassSector
    curate_orm_dependency = orms.Sector


class GlassOrganisationSector(CurateDependentOnGlassSector):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        data = mf.Run(f"GlassMainDumpFlow/{run_id}").data

        sector = data.sector.pipe(make_sector)

        return (
            data.sector.rename(
                columns={"id_organisation": "org_id", "sector_rank": "rank"}
            )
            .merge(sector, on="sector_name")[["org_id", "sector_id", "rank"]]
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
            )
        ).to_dict(orient="records")


class GlassOrganisationCompaniesHouseMatch(CurateDependentOnGlassOrganisation):
    def curate_data(self, s3path):
        run_id = Path(s3path).name
        return (
            mf.Run(f"GlassMainDumpFlow/{run_id}")
            .data.glass_company_match.rename(
                columns={
                    "id_organisation": "org_id",
                    "company_number": "company_id",
                    "company_number_match_type": "company_match_type",
                }
            )
            .dropna()
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
            )
        ).to_dict(orient="records")


class RootTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())
    production = luigi.BoolParameter(default=False)
    dump_date = luigi.Parameter(description="Metaflow input: format 'MM/YYYY'")
    s3_inputs = luigi.Parameter(
        description="Metaflow JSON input: requires keys `organisation`, `address`, `sector`"
    )

    def requires(self):
        tasks = [
            (GlassOrganisationMetadata, orms.OrganisationMetadata),
            (GlassOrganisationDescription, orms.OrganisationDescription),
            (GlassOrganisationSector, orms.OrganisationSector),
            # (
            #     GlassOrganisationCompaniesHouseMatch,
            #     orms.OrganisationCompaniesHouseMatch,
            # ),
            (AddressCurateTask, orms.OrganisationAddress),
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
        dump_date=self.dump_date,
        flow_path="glass_ai/ingest_data_dump.py",
        flow_kwargs={
            "s3_inputs": self.s3_inputs,
            "test_mode": test,
            "date": self.dump_date,
        },
        preflow_kwargs={"datastore": "s3", "package-suffixes": ".txt"},
        test=test,
        s3_inputs=self.s3_inputs,
    )
