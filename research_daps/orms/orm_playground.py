"""FOR DRAFTING ONLY - refactor experiments to tests!"""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from research_daps.orms.glass import (
    Base,
    Organisation,
    OrganisationDescription,
    OrganisationAddress,
    OrganisationSector,
    Address,
    Sector,
)


if __name__ == "__main__":
    from sqlalchemy import __version__

    print(__version__)

    engine = create_engine("sqlite:///example.db")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    date_1 = datetime.strptime("012020", "%m%Y")
    date_2 = datetime.strptime("022020", "%m%Y")

    address_no_id1 = Address(address_text="58 VE", postcode="Ne5 7A")
    address_no_id2 = Address(address_text="58 VEmbankment", postcode="Ne5 7A")

    org = Organisation(org_id=0, name="acme", website="http://...", active=True)

    # Descriptions
    description = OrganisationDescription(
        description="what we do",
        org_id=org.org_id,
        date=date_2,
    )
    description2 = OrganisationDescription(
        description="what we do",
        org_id=org.org_id,
        date=date_1,
    )

    # Associate address to organisation
    address = Address(address_id=0, address_text="58 VE", postcode="Ne5 7A")
    org.addresses = [OrganisationAddress(address_id=0, date=date_1, rank=1)]

    # Associate sector to organisation
    sector = Sector(sector_id=0, sector_name="AI buzzword sector")
    org.sectors = [OrganisationSector(sector_id=sector.sector_id, date=date_1, rank=1)]

    try:
        session.add_all([org, description, description2, address, sector])
    except Exception as e:
        print("Rollback")
        print(e)
        session.rollback()

    print("Descriptions for org: ", session.query(Organisation).first().descriptions)

    # Cumbersome and confusing(?) semantics
    print(
        "Date of association between org and address:",
        session.query(Address).first().organisations[0].date,
    )
    print(
        "Organisation related to address:",
        session.query(Address).first().organisations[0].organisation,
    )
    print(
        "Date of association between org and sector:",
        session.query(Sector).first().organisations[0].date,
    )
    print(
        "Organisation related to sector:",
        session.query(Sector).first().organisations[0].organisation,
    )

    print("*" * 20)
    sector = Sector(sector_id=1, sector_name="bureaucratic micro-management")
    org = Organisation(
        org_id=10, name="fizzbuzz.ai", website="fizzbuzz.ai", active=False
    )
    os = OrganisationSector(date=date_1, rank=1)
    org.sectors = [os]
    sector.organisations = [os]
    try:
        session.add_all([org, sector, os])
        print("OrgSector sector_id before commit:", os.sector_id)
        session.commit()
        print("OrgSector sector_id after commit:", os.sector_id)

        print("OrgSector list:", session.query(OrganisationSector).all())
        print(
            "Organisation related to sector:",
            session.query(Sector)
            .filter(Sector.sector_id == 1)
            .first()
            .organisations[0]
            .organisation.name,
        )
    except Exception as e:
        print(e)
        session.rollback()

    print("*" * 20)
    address = Address(address_text="bureaucratic micro-management", postcode="b00")
    os = OrganisationAddress(date=date_1, rank=1, org_id=org.org_id)
    os.address = address

    address2 = Address(address_text="bureaucratic micro-management", postcode="b00")
    os2 = OrganisationAddress(date=date_2, rank=1, org_id=org.org_id)
    os2.address = address2
    try:
        session.add_all([org, os, os2])
        print("OrgAddress address_id before commit:", os.address_id)
        session.commit()
        print("OrgAddress address_id after commit:", os.address_id)

        print("OrgAddress list:", session.query(OrganisationAddress).all())
        print(
            "Organisation related to address:",
            session.query(Address)
            .filter(Address.address_id == 1)
            .first()
            .organisations[0]
            .organisation.name,
        )
    except Exception as e:
        print(e)
        session.rollback()

# Association object pattern - From the docs:
# Working with the association pattern in its direct form requires that
# child objects are associated with an association instance before being
#  appended to the parent; similarly, access from parent to child goes
#  through the association object
