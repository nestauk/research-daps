# type: ignore
"""Companies House ORMs

Assumptions:

"""
from sqlalchemy import Table, Column, Integer, ForeignKey, Date, Boolean, VARCHAR, TEXT
from sqlalchemy.orm import relationship, backref
from research_daps import declarative_base

ADDRESS_TEXT_CHAR_LIM = 200
Base = declarative_base(prefix="companies_house_")


class CompanyAddress(Base):
    """Association object between organisations and addresses"""

    company_number = Column(
        VARCHAR(8),
        ForeignKey("companies_house_company.company_number"),
        primary_key=True,
    )
    address_id = Column(
        Integer, ForeignKey("companies_house_address.address_id"), primary_key=True
    )
    # is_active = Column(
    #     Boolean,
    #     nullable=False,
    #     index=True,
    #     doc="Indicates whether `address_id` is active",
    # )
    date = Column(
        Date,
        nullable=False,
        index=True,
        primary_key=True,
        doc="Date of data-dump associating org with address",
    )
    company = relationship("Company", back_populates="addresses")
    address = relationship("Address", back_populates="companies")


class CompanySector(Base):
    """Association object between organisations and sectors"""

    company_number = Column(
        VARCHAR(8),
        ForeignKey("companies_house_company.company_number"),
        primary_key=True,
    )
    sector_id = Column(
        Integer,
        ForeignKey("companies_house_sector.sector_id"),
        primary_key=True,
    )
    date = Column(
        Date,
        nullable=False,
        index=True,
        primary_key=True,
        doc="Date of data-dump associating org with sector",
    )
    rank = Column(
        Integer, nullable=False, index=True, doc="Rank of `sector_id` for `org_id`"
    )
    company = relationship("Company", back_populates="sectors")
    sector = relationship("Sector", back_populates="companies")


class Company(Base):
    """An organisation relating to a business website"""

    company_number = Column(
        VARCHAR(8), primary_key=True, doc="Companies house number", autoincrement=False
    )
    category = Column(VARCHAR(89), doc="")
    status = Column(VARCHAR(48), nullable=False, doc="")
    country_of_origin = Column(TEXT, nullable=False, doc="")
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
        TEXT,
        nullable=False,
        doc="URI of Company in Companies house API",
    )
    # One-to-many
    names = relationship("CompanyName", backref=backref("company"))
    # Many-to-many
    sectors = relationship("CompanySector", back_populates="company")
    addresses = relationship("CompanyAddress", back_populates="company")


class Address(Base):
    """List of companies registered addresses"""

    address_id = Column(Integer, primary_key=True, autoincrement=False)
    address_text = Column(VARCHAR(ADDRESS_TEXT_CHAR_LIM, collation="utf8mb4_bin"), unique=True, nullable=False)
    # line1 = Column(VARCHAR(100), unique=True)
    # line2 = Column(VARCHAR(100), unique=True)
    postcode = Column(VARCHAR(9), index=True)
    # care_of = Column(TEXT, primary_key=True)  # TODO: in link table?
    # pobox = Column(TEXT, primary_key=True)  # Not useful
    # town = Column(TEXT)  # Redundant
    # county = Column(TEXT)  # Redundant
    # country = Column(TEXT)  # Redundant

    companies = relationship("CompanyAddress", back_populates="address")
    # organisations = relationship(
    #     "Organisation", secondary=organisation_address, back_populates="addresses"
    # )


class CompanyName(Base):
    """Names of Companies"""

    company_number = Column(
        "company_number",
        VARCHAR(8),
        ForeignKey("companies_house_company.company_number"),
        primary_key=True,
    )
    name_age_index = Column(
        Integer,
        nullable=False,
        primary_key=True,
        doc="Age of name, where 0 corresponds to current name.",
    )
    name = Column(VARCHAR(150), nullable=False, index=True, doc="Companies house name")
    invalid_date = Column(
        Date,
        nullable=False,
        index=True,
        doc="Date the name changed to something else, or the latest data dump date if current name.",
    )
    # Relationship
    # company = relationship("Company", backref=backref("names"))
    # company = relationship("Company", back_populates="names")


class Sector(Base):
    """Sector names: LinkedIn taxonomy"""

    sector_id = Column(Integer, primary_key=True, autoincrement=False)
    sector_name = Column(
        VARCHAR(166),
        nullable=False,
        index=True,
        unique=True,
        doc="Name of sector (SIC5)",
    )
    companies = relationship(
        "CompanySector", back_populates="sector"
    )
