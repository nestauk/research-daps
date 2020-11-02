# type: ignore
"""Glass AI ORMs

Assumptions:
- Organisation names and URL's do not change
- Sectors do change [TODO]
- Addresses do change [TODO]

"""
from sqlalchemy import Table, Column, Integer, String, ForeignKey, Date, Boolean
from sqlalchemy.orm import relationship, backref
from research_daps import declarative_base

Base = declarative_base()

organisation_address = Table(
    "organisation_address",
    Base.metadata,
    Column("org_id", Integer, ForeignKey("organisation.org_id")),
    Column("address_id", Integer, ForeignKey("address.address_id")),
    # TODO: Active? boolean column indicating whether address is in latest data dump
    # TODO: Rank Integer column indicating address rank
    # TODO: Date Date column indicating date of latest dump with this relationship
)

organisation_sector = Table(
    "organisation_sector",
    Base.metadata,
    Column("org_id", Integer, ForeignKey("organisation.org_id")),
    Column("sector_id", Integer, ForeignKey("sector.sector_id")),
    # TODO: Active? boolean column indicating whether sector is in latest data dump
    # TODO: Rank Integer column indicating sector rank
    # TODO: Date Date column indicating date of latest dump with this relationship
)

notice_terms = Table(
    "notice_terms",
    Base.metadata,
    Column("notice_id", Integer, ForeignKey("notice.notice_id")),
    Column("term_id", Integer, ForeignKey("covid_term.term_id")),
)


class Organisation(Base):
    """An organisation relating to a business website"""

    org_id = Column(Integer, primary_key=True)
    name = Column(
        String,
        unique=True,
        doc="Organisation name inferred by named entity recognition",
        index=True,
    )
    website = Column(String, nullable=False, doc="URL of organisation")
    active = Column(
        Boolean,
        nullable=False,
        doc="True if Organisation was contained in last data-dump",
    )
    # One-to-many
    notices = relationship("Notice", backref=backref("organisation"))
    descriptions = relationship(
        "OrganisationDescription", backref=backref("organisation")
    )
    metadata_ = relationship("OrganisationMetadata", backref=backref("organisation"))
    # Many-to-many
    sectors = relationship(
        "Sector", secondary=organisation_sector, back_populates="organisations"
    )
    addresses = relationship(
        "Address", secondary=organisation_address, back_populates="organisations"
    )


class OrganisationMetadata(Base):
    """Organisation metadata which may not be stable over time"""

    org_id = Column(Integer, ForeignKey("organisation.org_id"), primary_key=True)
    date = Column(
        Date, primary_key=True, doc="Date of data-dump inserting row", index=True
    )
    has_webshop = Column(
        Boolean, nullable=False, doc="If True, presence of a webshop was found"
    )
    vat_number = Column(String, doc="VAT number (low population)")
    low_quality = Column(
        Boolean,
        nullable=False,
        doc="If True, information for `org_id` was of low quality",
    )


class OrganisationDescription(Base):
    """Descriptions of organisation activities"""

    # description_id = Column(Integer, primary_key=True)
    org_id = Column(Integer, ForeignKey("organisation.org_id"), primary_key=True)
    description = Column(
        String, nullable=False, doc="Description of organisation extracted from website"
    )
    date = Column(
        Date,
        nullable=False,
        primary_key=True,
        index=True,
        doc="Date of data-dump inserting row",
    )


class Address(Base):
    """List of addresses found in websites"""

    address_id = Column(Integer, primary_key=True)
    address_text = Column(String, nullable=False, unique=True, doc="Full address text")
    postcode = Column(String, doc="Postcode of address")
    organisations = relationship(
        "Organisation", secondary=organisation_address, back_populates="addresses"
    )


class Notice(Base):
    """Covid Notices extracted from websites"""

    notice_id = Column(Integer, primary_key=True)
    org_id = Column("org_id", Integer, ForeignKey("organisation.org_id"))
    snippet = Column(
        String, nullable=False, doc="Extracted text snippet relating to COVID"
    )
    url = Column(String, nullable=False, doc="URL snippet was extracted from")
    date = Column(
        Date, nullable=False, doc="Date of data-dump inserting row", index=True
    )
    terms = relationship("CovidTerm", secondary=notice_terms, back_populates="notices")


class CovidTerm(Base):
    """Set of terms relating to Covid-19, curated by Glass.AI
    Terms are used to find notices
    """

    term_id = Column(Integer, primary_key=True)
    term_string = Column(String, unique=True, index=True)
    date_introduced = Column(
        Date, doc="Date of data-dump term was first used to find notices"
    )
    notices = relationship("Notice", secondary=notice_terms, back_populates="terms")


class Sector(Base):
    """Sector names: LinkedIn taxonomy"""

    sector_id = Column(Integer, primary_key=True)
    sector_name = Column(String, unique=True, doc="Name of sector")
    organisations = relationship(
        "Organisation", secondary=organisation_sector, back_populates="sectors"
    )


class OrganisationCompaniesHouseMatch(Base):
    """Organisation matches to companies house performed by Glass"""

    company_id = Column(
        String, doc="Companies House number", primary_key=True
    )  # TODO: Add `ForeignKey("companies.company_id")` once companies house pipeline / ORM added
    org_id = Column(
        "org_id", Integer, ForeignKey("organisation.org_id"), primary_key=True
    )
    company_match_type = Column(String, doc="Type of match: MATCH_3,MATCH_4,MATCH_5")
