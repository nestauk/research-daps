import datetime
import metaflow as mf
import luigi
from luigi.contrib.s3 import S3PathTask
from luigi.contrib.mysqldb import MySqlTarget
from sqlalchemy_utils.functions import get_declarative_base
from sqlalchemy.sql.expression import func
from numpy import nan

from daps_utils import CurateTask
from daps_utils.parameters import SqlAlchemyParameter
from daps_utils.db import db_session
# from daps_utils.tasks import MetaflowTask
import research_daps
import research_daps.orms.glass as orms


class GlassOrganisation(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        # TODO: Set every org to inactive

        # Get glass flow data
        data = mf.Flow("GlassMainDumpFlow").latest_successful_run.data.organisation

        # TODO: Filter columns - query against existing

        # Output as dict?
        return (
            data.rename(
                columns={"id_organisation": "org_id", "organisation_name": "name"}
            )
            .assign(active=True)[["org_id", "name", "website", "active"]]
            .to_dict(orient="records")
        )


class GlassOrganisationMetadata(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        return (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.organisation.rename(
                columns={"id_organisation": "org_id"}
            )
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
                low_quality=lambda x: x.low_quality.fillna(False),
                vat_number=lambda x: x.vat_number.replace({nan: None}),
            )[["org_id", "date", "has_webshop", "vat_number", "low_quality"]]
            .to_dict(orient="records")
        )


class GlassOrganisationDescription(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        return (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.organisation.rename(
                columns={"id_organisation": "org_id"}
            )
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
            )[["org_id", "date", "description"]]
            .dropna()
        ).to_dict(orient="records")


class GlassAddress(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        return (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.address[["address_text", "postcode"]]
            .drop_duplicates()
            .loc[lambda x: ~x.address_text.str.lower().duplicated(keep=False)]
        ).to_dict(orient="records")


class GlassOrganisationAddress(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        links = (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.address.rename(
                columns={
                    "id_organisation": "org_id",
                    "address_rank": "rank",
                }
            )
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
                address_id=lambda x: x.index,
            )[["org_id", "address_id", "date", "rank"]]
        )

        return links.to_dict(orient="records")


class AddressCurateTask(luigi.Task):
    """Run metaflow Flows in Docker, then curate the data
    and store the result in a database table.
    Args:
        model (SqlAlchemy model): A SqlAlchemy ORM, indicating the table
                                  of interest.
        flow_path (str): Path to your flow, relative to the flows directory.
        rebuild_base (bool): Whether or not to rebuild the docker image from
                             scratch (starting with Dockerfile-base then
                             Dockerfile). Only do this if you have changed
                             Dockerfile-base.
        rebuild_flow (bool): Whether or not to rebuild the docker image from
                             the base image upwards (only implementing
                             Dockerfile, not Dockerfile-base). This is done by
                             default to include the latest changes to your flow
        flow_kwargs (dict): Keyword arguments to pass to your flow as
                            parameters (e.g. `{'foo':'bar'}` will be passed to
                            the flow as `metaflow example.py run --foo bar`).
        preflow_kwargs (dict): Keyword arguments to pass to metaflow BEFORE the
                               run command (e.g. `{'foo':'bar'}` will be passed
                               to the flow as `metaflow example.py --foo bar run`).
        container_kwargs (dict): Additional keyword arguments to pass to the
                                 docker run command, e.g. mem_limit for setting
                                 the memory limit. See the python-docker docs
                                 for full information.
        requires_task (luigi.Task): Any task that this task is dependent on.
        requires_task_kwargs (dict): Keyword arguments to pass to any dependent
                                     task, if applicable.
    """

    orm = SqlAlchemyParameter()
    flow_path = luigi.Parameter()
    rebuild_base = luigi.BoolParameter(default=False)
    rebuild_flow = luigi.BoolParameter(default=True)
    flow_kwargs = luigi.DictParameter(default={})
    preflow_kwargs = luigi.DictParameter(default={})
    container_kwargs = luigi.DictParameter(default={})
    requires_task = luigi.TaskParameter(default=S3PathTask)
    requires_task_kwargs = luigi.DictParameter(default={})
    low_memory = luigi.BoolParameter(default=True)
    test = luigi.BoolParameter(default=True)
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()
    s3_inputs = luigi.Parameter()

    def curate_data(self):
        links = (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.address.rename(
                columns={
                    "id_organisation": "org_id",
                    "address_rank": "rank",
                }
            )
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
                # address_id=lambda x: x.index
            )
            # [['org_id', 'address_id', 'date', 'rank']]
        )

        return links.to_dict(orient="records")

    def requires(self):
        # tag = "dev" if self.test else "production"
        return GlassOrganisation(
                date=self.date,
                dump_date=self.dump_date,
                orm=orms.Organisation,
                flow_path="glass_ai/ingest_data_dump.py",
                flow_kwargs={
                    "s3_inputs": self.s3_inputs,
                    "test_mode": self.test,
                    "date": self.dump_date,
                },
                preflow_kwargs={"datastore": "s3", "package-suffixes": ".txt"},
            )

    def run(self):
        data = self.curate_data()
        with db_session(database="dev" if self.test else "production") as session:
            # Create the table if it doesn't already exist
            engine = session.get_bind()
            Base = get_declarative_base(self.orm)
            Base.metadata.create_all(engine)

            def process(row):

                # Find existing address_text's and get ID's
                r = (
                    session.query(orms.Address)
                    .filter(orms.Address.address_text == row["address_text"])
                    .one_or_none()
                )
                if r:  # Get address_id to build link with
                    print("result:", r)
                    address_id = r.address_id
                else:  # Add new Address
                    address = orms.Address(
                        address_text=row["address_text"],
                        postcode=row["postcode"],
                        # address_id=uuid.uuid4().int,
                    )
                    session.add(address)
                    session.commit()
                    address_id = address.address_id

                link = orms.OrganisationAddress(
                    org_id=row["org_id"],
                    address_id=address_id,
                    date=row["date"],
                    rank=row["rank"],
                )
                session.add(link)
                session.commit()

            # list(map(process, data))
            def process2(row):

                link = orms.OrganisationAddress(
                    org_id=row["org_id"],
                    date=row["date"],
                    rank=row["rank"],
                )

                # Find any existing Address
                address = (
                    session.query(orms.Address)
                    .filter(orms.Address.address_text == row["address_text"])
                    .one_or_none()
                ) or orms.Address(
                    address_text=row["address_text"],
                    postcode=row["postcode"],
                )
                link.address = address

                session.add(link)

                return

            # session.add_all(map(process2, data))
            # list(map(process2, data))
            # session.commit()

            existing_addresses = {addr.address_text: addr.address_id for addr in session.query(orms.Address).all()}  # filter(orms.Address.address_text.in_(data.address_text.values))
            counter = session.query(func.max(orms.Address.address_id)).scalar() or 1
            links = []
            print(len(existing_addresses))
            for row in data:
                link = orms.OrganisationAddress(
                    org_id=row["org_id"],
                    date=row["date"],
                    rank=row["rank"],
                )

                address_id = existing_addresses.get(row["address_text"].lower(), None)
                if not address_id:
                    counter += 1
                    address = orms.Address(
                        address_id=counter,
                        address_text=row["address_text"],
                        postcode=row["postcode"],
                    )
                    link.address = address
                    existing_addresses[row["address_text"].lower()] = counter
                    # print(counter)
                    # print(row["address_text"])

                links.append(link)
            print(len(data))
            print(len(links))
            print(len(existing_addresses))
            session.add_all(links)

            # Need to generate lookup from address_text to Address

        return self.output().touch()

    def output(self):
        conf = research_daps.config["mysqldb"]["mysqldb"]
        conf["database"] = "dev" if self.test else "production"
        conf["table"] = "BatchExample"
        if "port" in conf:
            conf.pop("port")
        return MySqlTarget(update_id=self.task_id, **conf)


class GlassOrganisationSector(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        return (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.organisation.rename(
                columns={"id_organisation": "org_id", "organisation_name": "name"}
            )
            .assign(
                date=datetime.datetime.strptime(self.dump_date, "%m/%Y").date(),
            )
        ).to_dict(orient="records")


class GlassSector(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        return (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.sector[["sector_name"]]
            .drop_duplicates()
            .sort_values("sector_name")
            .reset_index(drop=True)  # Start from zero
            .assign(sector_id=lambda x: x.index + 1)
        ).to_dict(orient="records")


class GlassOrganisationCompaniesHouseMatch(CurateTask):
    date = luigi.DateParameter()
    dump_date = luigi.Parameter()

    def curate_data(self, _):
        return (
            mf.Flow("GlassMainDumpFlow")
            .latest_successful_run.data.organisation.rename(
                columns={"id_organisation": "org_id", "organisation_name": "name"}
            )
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
            # (GlassOrganisation, orms.Organisation),
            # (GlassOrganisationMetadata, orms.OrganisationMetadata),
            # (GlassOrganisationDescription, orms.OrganisationDescription),
            (GlassSector, orms.Sector),
            # (GlassAddress, orms.Address),
            # (GlassOrganisationAddress, orms.OrganisationAddress),
            # (GlassOrganisationSector, orms.OrganisationSector),
            # (GlassOrganisationCompaniesHouseMatch, orms.OrganisationCompaniesHouseMatch),
        ]

        for Task_, Orm_ in tasks:
            yield Task_(
                date=self.date,
                dump_date=self.dump_date,
                orm=Orm_,
                flow_path="glass_ai/ingest_data_dump.py",
                flow_kwargs={
                    "s3_inputs": self.s3_inputs,
                    "test_mode": not self.production,
                    "date": self.dump_date,
                },
                preflow_kwargs={"datastore": "s3", "package-suffixes": ".txt"},
            )
        yield AddressCurateTask(
            date=self.date,
            dump_date=self.dump_date,
            orm=orms.OrganisationAddress,
            flow_path="glass_ai/ingest_data_dump.py",
            s3_inputs=self.s3_inputs,
            preflow_kwargs={"datastore": "s3", "package-suffixes": ".txt"},
        )
