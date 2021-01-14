# type: ignore
"""Glass AI ORMs

Assumptions:
- organisation.name does not change
- organisation.website does not change
- Sector ORM does not change

Longitudinally updating ORM's (via. append) tracking time:
- OrganisationAddress
- OrganisationSector
- OrganisationDescription
- OrganisationCompaniesHouseMatch
- OrganisationMetadata
- CovidTerm
- Notice

Updates:
- Organisation.active

"""
from sqlalchemy import (
    Table,
    Column,
    Integer,
    TEXT,
    ForeignKey,
    Date,
    Boolean,
    VARCHAR,
    CHAR,
)
from sqlalchemy.orm import relationship, backref
from research_daps import declarative_base

ADDRESS_TEXT_CHAR_LIM = 300
Base = declarative_base(prefix="glass_")


class OrganisationAddress(Base):
    """Association object between organisations and addresses"""

    org_id = Column(
        Integer,
        ForeignKey("glass_organisation.org_id"),
        primary_key=True,
    )
    address_id = Column(
        Integer,
        ForeignKey("glass_address.address_id"),
        primary_key=True,
    )
    date = Column(
        Date,
        nullable=False,
        index=True,
        primary_key=True,
        doc="Date of data-dump associating org with address",
    )
    rank = Column(
        Integer, nullable=False, index=True, doc="Rank of `address_id` for `org_id`"
    )
    organisation = relationship("Organisation", back_populates="addresses")
    address = relationship("Address", back_populates="organisations")


class OrganisationSector(Base):
    """Association object between organisations and sectors"""

    org_id = Column(
        Integer,
        ForeignKey("glass_organisation.org_id"),
        primary_key=True,
    )
    sector_id = Column(
        Integer,
        ForeignKey("glass_sector.sector_id"),
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
    organisation = relationship("Organisation", back_populates="sectors")
    sector = relationship("Sector", back_populates="organisations")


notice_terms = Table(
    "glass_notice_terms",
    Base.metadata,
    Column("notice_id", Integer, ForeignKey("glass_notice.notice_id")),
    Column("term_id", Integer, ForeignKey("glass_covid_term.term_id")),
)


class Organisation(Base):
    """An organisation relating to a business website"""

    org_id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(
        VARCHAR(200),  # AB 10/11/20: 167 is longest name
        # unique=True,  # AB 18/12/20 names can be duplicated with different PK's
        doc="Organisation name inferred by named entity recognition",
        index=True,
    )
    website = Column(TEXT, nullable=False, doc="URL of organisation")
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
    addresses = relationship("OrganisationAddress", back_populates="organisation")
    sectors = relationship("OrganisationSector", back_populates="organisation")
    company_match_by_glass = relationship(
        "OrganisationCompaniesHouseMatch", back_populates="organisation"
    )


class OrganisationMetadata(Base):
    """Organisation metadata which may not be stable over time"""

    org_id = Column(
        Integer,
        ForeignKey("glass_organisation.org_id"),
        primary_key=True,
        autoincrement=False,
    )
    date = Column(
        Date, primary_key=True, doc="Date of data-dump inserting row", index=True
    )
    has_webshop = Column(
        Boolean, nullable=False, doc="If True, presence of a webshop was found"
    )
    vat_number = Column(CHAR(9), doc="VAT number (high null count)")
    low_quality = Column(
        Boolean,
        nullable=False,
        doc="If True, information for `org_id` was of low quality",
    )


class OrganisationDescription(Base):
    """Descriptions of organisation activities"""

    org_id = Column(
        Integer,
        ForeignKey("glass_organisation.org_id"),
        primary_key=True,
        autoincrement=False,
    )
    date = Column(
        Date,
        nullable=False,
        primary_key=True,
        index=True,
        doc="Date of data-dump inserting row",
    )
    description = Column(
        TEXT, nullable=False, doc="Description of organisation extracted from website"
    )


class Address(Base):
    """List of addresses found in websites"""

    address_id = Column(Integer, primary_key=True, autoincrement=False)
    address_text = Column(
        VARCHAR(ADDRESS_TEXT_CHAR_LIM, collation="utf8mb4_bin"),
        nullable=False,
        doc="Full address text",
        unique=True,
    )  # AB 10/11/20: 266 longest
    postcode = Column(VARCHAR(8), index=True, doc="Postcode of address")
    organisations = relationship("OrganisationAddress", back_populates="address")

    def __repr__(self):
        return f"Address: {self.__dict__}"


class Notice(Base):
    """Covid Notices extracted from websites"""

    notice_id = Column(Integer, primary_key=True)
    org_id = Column(Integer, ForeignKey("glass_organisation.org_id"))
    date = Column(
        Date, nullable=False, index=True, doc="Date of data-dump inserting row"
    )
    snippet = Column(
        TEXT, nullable=False, doc="Extracted text snippet relating to COVID"
    )
    url = Column(TEXT, nullable=False, doc="URL snippet was extracted from")
    terms = relationship("CovidTerm", secondary=notice_terms, back_populates="notices")


class CovidTerm(Base):
    """Set of terms relating to Covid-19, curated by Glass.AI
    Terms are used to find notices
    """

    term_id = Column(Integer, primary_key=True, autoincrement=False)
    term_string = Column(
        VARCHAR(100), nullable=False, unique=True, index=True
    )  # TODO length exploration
    date = Column(
        Date,
        nullable=False,
        doc="Date of data-dump term was first used to find notices",
    )
    notices = relationship("Notice", secondary=notice_terms, back_populates="terms")


class Sector(Base):
    """Sector names: LinkedIn taxonomy"""

    sector_id = Column(Integer, primary_key=True, autoincrement=False)
    sector_name = Column(
        VARCHAR(43), nullable=False, index=True, unique=True, doc="Name of sector"
    )
    organisations = relationship("OrganisationSector", back_populates="sector")


class OrganisationCompaniesHouseMatch(Base):
    """Organisation matches to companies house performed by Glass (Association object)"""

    company_id = Column(
        VARCHAR(8), doc="Companies House number", primary_key=True
    )  # TODO: Add `ForeignKey("glass_companies.company_id")` once companies house pipeline / ORM added
    org_id = Column(
        "org_id", Integer, ForeignKey("glass_organisation.org_id"), primary_key=True
    )
    date = Column(
        Date,
        nullable=False,
        index=True,
        # primary_key=True,
        doc="Date of data-dump associating company_id with org_id",
    )
    company_match_type = Column(
        TEXT, nullable=False, doc="Type of match: MATCH_3,MATCH_4,MATCH_5"
    )
    organisation = relationship("Organisation", back_populates="company_match_by_glass")
