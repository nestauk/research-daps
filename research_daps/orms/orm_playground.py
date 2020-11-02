"""FOR DRAFTING ONLY - refactor experiments to tests!"""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from glass import (
    Base,
    Organisation,
    OrganisationDescription,
    Address,
    OrganisationAddress,
    Sector,
)


if __name__ == "__main__":
    engine = create_engine("sqlite:///example.db")
    Session = sessionmaker(bind=engine)
    session = Session()

    date_1 = datetime.strptime("012020", "%m%Y")
    date_2 = datetime.strptime("022020", "%m%Y")

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
    org.addresses = [OrganisationAddress(address_id=0, date=date_1)]

    # Associate sector to organisation
    sector = Sector(sector_name="AI buzzword sector")
    org.sectors = [sector]

    Base.metadata.create_all(engine)

    try:
        session.add_all([org, description, description2, address])
    except Exception as e:
        print("Rollback")
        print(e)
        session.rollback()

    print("Descriptions for org: ", session.query(Organisation).first().descriptions)

    # Cumbersome and confusing(?) semantics
    # when compared to sectors which doesn't use an association object:
    print(
        "Date of association between org and address:",
        session.query(Address).first().organisations[0].date,
    )
    print(
        "Organisation related to address:",
        session.query(Address).first().organisations[0].organisation,
    )

    print(
        "Organisation related to sector:", session.query(Sector).first().organisations
    )


# Association object pattern - From the docs:
# Working with the association pattern in its direct form requires that
# child objects are associated with an association instance before being
#  appended to the parent; similarly, access from parent to child goes
#  through the association object
