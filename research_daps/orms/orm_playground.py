"""FOR DRAFTING ONLY - refactor experiments to tests!"""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from glass import Base, Organisation, OrganisationDescription


if __name__ == "__main__":
    engine = create_engine("sqlite:///example.db")
    Session = sessionmaker(bind=engine)
    session = Session()

    o = Organisation(org_id=0, name="acme", website="http://...", active=True)
    description = OrganisationDescription(
        description="what we do",
        org_id=o.org_id,
        date=datetime.strptime("012020", "%m%Y"),
    )
    description2 = OrganisationDescription(
        description="what we do",
        org_id=o.org_id,
        date=datetime.strptime("022020", "%m%Y"),
    )

    Base.metadata.create_all(engine)

    try:
        session.add_all([o, description, description2])
    except Exception as e:
        print("Rollback")
        print(e)
        session.rollback()

    print(session.query(Organisation).first().descriptions)
