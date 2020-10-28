import sqlite3
import pandas as pd
import metaflow as mf
import toolz.curried as t
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from research_daps.orms.glass import *


def find_flow():
    """Find metaflow run relating to Flow and parameters"""
    pass


if __name__ == "__main__":
    engine = create_engine("sqlite:///example.db")
    Session = sessionmaker(bind=engine)
    session = Session()

    Base.metadata.create_all(engine)

    data = t.pipe(
        mf.Flow("GlassMainDumpFlow").runs(),
        t.filter(lambda x: x.successful),
        t.map(lambda x: x.data),
        t.filter(lambda x: x is not None),
        # t.filter(lambda x: x is not None and (not x.test_mode)),
        next,
    )
    print(data)

    sector_cat_type = pd.CategoricalDtype(
        data.sector.sector_name.drop_duplicates().sort_values(), ordered=True
    )
    sector_ = data.sector.assign(
        sector_id=lambda x: x.sector_name.astype(sector_cat_type).cat.codes
    )
    sector = sector_[["sector_id", "sector_name"]]
    organisation_sector = sector_[["id_organisation", "sector_id", "sector_rank"]]

    address_cat_type = pd.CategoricalDtype(
        data.address.address_text.drop_duplicates().sort_values(), ordered=True
    )
    address_ = data.address.assign(
        address_id=lambda x: x.address_text.astype(address_cat_type).cat.codes
    )
    address = address_[["address_id", "address_text", "postcode"]]
    organisation_address = address_[["org_id", "address_id", "address_rank"]]

    organisation_ = data.organisation.rename(
        columns={"id_organisation": "org_id", "organisation_name": "name"}
    ).assign(active=True, date=data.date)
    organisation = organisation_[["org_id", "name", "website", "active"]]
    organisation_metadata = organisation_[
        ["org_id", "date", "has_webshop", "vat_number", "low_quality"]
    ]
    organisation_description = organisation_[["org_id", "description", "date"]]

    # Check sector table unchanged
    # Add new addresses to address
    #
    # TRANSACTION:
    # Set all orgs to inactive
    # For each org:
    # - Add Organisation
    # - Add OrganisationDescription (if exists update date)
    # - Add OrganisationMetadata (if exists update date)
    #
    # Update address relationship
    # Update sector relationship
    #
    # Add new covid terms
    # Add new notices (implicitly update notice relationship)

    df = (
        data.organisation.head(100)
        .rename(columns={"id_organisation": "org_id", "organisation_name": "name"})
        .assign(active=True, date=data.date)
        .dropna(subset=["description"])
    )
    records = df.to_dict(orient="records")
    for org in records:
        d = OrganisationDescription(
            **t.pipe(org, t.keyfilter(lambda x: x in ["org_id", "description", "date"]))
        )

        o = Organisation(
            **t.pipe(
                org, t.keyfilter(lambda x: x in ["org_id", "name", "website", "active"])
            ),
            descriptions=[d],
        )

    print(o)
