# type: ignore
"""Companies House ORMs

Assumptions:

"""
from sqlalchemy import Table, Column, Integer, String, ForeignKey, Date, Boolean
from sqlalchemy.orm import relationship, backref
from research_daps import declarative_base

Base = declarative_base()


class OrganisationAddress(Base):
    """Association object between organisations and addresses"""

    org_id = Column(Integer, ForeignKey("organisation.org_id"), primary_key=True)
    address_id = Column(Integer, ForeignKey("address.address_id"), primary_key=True)
    # TODO: do we really want to keep ranks? If ranks constantly change then
    # this proliferates a lot of data
    rank = Column(
        Integer, nullable=False, index=True, doc="Rank of `address_id` for `org_id`"
    )
    is_active = Column(
        Boolean,
        nullable=False,
        index=True,
        doc="Indicates whether `address_id` is active",
    )
    # TODO : should this really have two date columns: valid_from and valid_to ?
    date = Column(
        Date,
        nullable=False,
        index=True,
        doc="Date of data-dump associating org with address",
    )
    organisation = relationship("Organisation", back_populates="addresses")
    address = relationship("Address", back_populates="organisations")


organisation_sector = Table(
    "organisation_sector",
    Base.metadata,
    Column("org_id", Integer, ForeignKey("organisation.org_id")),
    Column("sector_id", Integer, ForeignKey("sector.sector_id")),
    # TODO: Active? boolean column indicating whether sector is in latest data dump
    # TODO: Rank Integer column indicating sector rank
    # TODO: Date Date column indicating date of latest dump with this relationship
)


class Company(Base):
    """An organisation relating to a business website"""

    company_number = Column(String, primary_key=True, doc="Companies house number")
    category = Column(String, doc="")
    status = Column(String, nullable=False, doc="")
    country_of_origin = Column(String, nullable=False, doc="")
    dissolution_date = Column(
        Date,
        doc="Dissolution date (mostly null)",
    )
    incorporation_date = Column(
        Date,
        nullable=False,
        doc="Incorporation date",
    )
    uri = Column(
        String,
        nullable=False,
        doc="URI of Company in Companies house API",
    )
    # One-to-many
    names = relationship("CompanyName", backref=backref("company"))
    # Many-to-many
    # sectors = relationship(
    #     "Sector", secondary=organisation_sector, back_populates="company"
    # )
    # addresses = relationship("OrganisationAddress", back_populates="company")


class Address(Base):
    """List of companies registered addresses"""

    address_id = Column(Integer, primary_key=True)
    address_text = Column(String, nullable=False, unique=True, doc="Full address text")
    postcode = Column(String, doc="Postcode of address")
    organisations = relationship("OrganisationAddress", back_populates="address")
    # organisations = relationship(
    #     "Organisation", secondary=organisation_address, back_populates="addresses"
    # )


class CompanyName(Base):
    """Names of Companies"""

    company_id = Column("company_id", String, ForeignKey("company.company_id"), primary_key=True)
    name_age_index = Column(
        Integer, nullable=False, primary_key=True, doc="Age of name, where 0 corresponds to current name."
    )
    name = Column(String, nullable=False, index=True, doc="Companies house name")
    change_date = Column(Date, nullable=False, index=True, doc="Date of name-change")
    # Relationship
    company = relationship("Company", backref=backref("names"))


class Sector(Base):
    """Sector names: LinkedIn taxonomy"""

    sector_id = Column(Integer, primary_key=True)
    sector_name = Column(
        String, nullable=False, index=True, unique=True, doc="Name of sector"
    )
    organisations = relationship(
        "Organisation", secondary=organisation_sector, back_populates="sectors"
    )
