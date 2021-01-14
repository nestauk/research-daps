""" Functions to process raw Glass AI data into dataframes """
import csv
import datetime
import logging
import re
from pathlib import Path
from typing import Union, List, IO

import pandas as pd

# from smart_open import smart_open
from toolz import merge

import bulwark.decorators as dc
import bulwark.checks as ck


def postcode_extractor(x: pd.Series, all: bool = False) -> pd.Series:
    """Extract postcodes from a Series using regex (case insensitive)
    Constructed from:
    https://stackoverflow.com/questions/164979/regex-for-matching-uk-postcodes/51885364#51885364
    Args:
        x (pandas.Series): Input series of str
        all (bool, optional): If True, extract all
    Returns:
        pandas.DataFrame
    """
    postcode_regex = (
        r"(?P<postcode>([A-Z]{1,2}\d[A-Z\d]?|ASCN|STHL|TDCU|BBND|[BFS]IQQ|PCRN|TKCA) "
        r"?\d[A-Z]{2}|BFPO ?\d{1,4}|(KY\d|MSR|VG|AI)[ -]?\d{4}"
        r"|[A-Z]{2} ?\d{2}|GE ?CX|GIR ?0A{2}|SAN ?TA1)"
    )
    if all:
        extractor = x.str.extractall
    else:
        extractor = x.str.extract
    return extractor(postcode_regex, flags=re.IGNORECASE)["postcode"]


SHORT_DESCRIPTION_THRESHOLD = 50
LONG_NAME_THRESHOLD = 100
FREQUENT_DESCRIPTION_THRESHOLD = 50


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ English UK columns """
    return df.rename(columns=lambda x: re.sub("organization", "organisation", x))


# class ReadPreHook(object):
#     """Run `prehook` on `smart_open`'able `file`."""

#     def __init__(self, file, prehook):
#         self.file = smart_open(file)
#         self.prehook = prehook

#     def __next__(self):
#         return self.next()

#     def __iter__(self):
#         return self

#     def read(self, *args, **kwargs):
#         return self.__next__()

#     def next(self):
#         try:
#             line: str = self.file.readline()
#             return self.prehook(line)
#         except Exception:
#             self.file.close()
#             raise StopIteration


def glass_df_loader(filepath_or_buffer: Union[str, Path, IO], **kwargs) -> pd.DataFrame:
    """ Wrapper around `pandas.read_csv` to load glass file format and dtypes """
    dtypes = {"vat_number": object}
    parsing = {
        "sep": "\t",
        "engine": "c",
        "lineterminator": "\n",
        "quoting": csv.QUOTE_NONE,
    }
    kwargs["dtype"] = merge(kwargs.get("dtype", {}), dtypes)
    kwargs = merge(kwargs, parsing)

    return pd.read_csv(filepath_or_buffer, **kwargs).pipe(_rename_columns)


def _contains_uniques(df: pd.DataFrame, columns: List[str]) -> bool:
    """ Return True if each of `columns` in `df` have values appearing only once """
    return not any(map(lambda x: df[x].duplicated().any(), columns))


@dc.HasNoNans(
    [
        "id_organisation",
        "organisation_name",
        "website",
    ]
)
@dc.HasDtypes(
    {
        "id_organisation": int,
        "has_webshop": bool,
        "vat_number": object,  # Has leading zeros
    }
)
@dc.CustomCheck(_contains_uniques, ["id_organisation"])
def process_organisations(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """ Process organisation table """

    df = (
        df.set_index("id_organisation")
        # Drop company information that belongs in Companies House
        .drop(["registered_name", "registered_postcode"], axis=1)
        # Give clearer names
        .rename(
            columns={
                "crn": "company_number",
                "ch_match_type": "company_number_match_type",
            }
        ).assign(
            has_webshop=lambda x: x.has_webshop.map({"Y": True}),
        )
        # Fill missing values
        .fillna(
            {"has_webshop": False},
            downcast="infer",
        )
    )
    return df.assign(
        # Website as backup Organisation name
        organisation_name=lambda x: x.organisation_name.combine_first(x.website),
        # Int to bool
        has_webshop=lambda x: x.has_webshop.astype(bool),
    ).reset_index()


def check_organisations(
    df: pd.DataFrame,
    n_address: pd.Series,
    n_sectors: pd.Series,
) -> pd.DataFrame:
    """ Process organisation table """

    df = (
        df.set_index("id_organisation")
        # Add new columns from other tables
        .join(n_address)
        .join(n_sectors)
        .assign(
            len_description=lambda x: x.description.str.len(),  # Description length
        )
        # Fill missing values
        .fillna(
            {
                "n_address": 0,
                "n_sectors": 0,
                "len_description": 0,
            },
            downcast="infer",
        )
        .pipe(
            ck.within_range,
            {
                "n_address": (0, 5),
                "n_sectors": (0, 7),
            },
        )
    )

    # Various low quality flags
    description_frequency_quality = (  # Flag frequent descriptions
        df.merge(
            df.description.value_counts().to_frame("description_frequency_flag"),
            left_on="description",
            right_index=True,
            how="left",
        ).description_frequency_flag.fillna(0)
        > FREQUENT_DESCRIPTION_THRESHOLD
    )
    name_quality = df.organisation_name.isnull() | (
        df.organisation_name.str.len() > LONG_NAME_THRESHOLD
    )
    description_quality = (
        df.description.isnull()
        | (df.len_description < SHORT_DESCRIPTION_THRESHOLD)
        | description_frequency_quality
    )
    address_quality = df.n_address == 0
    sector_quality = df.n_sectors == 0

    return (
        name_quality | description_quality | address_quality | sector_quality
    ).rename("low_quality")


@dc.HasDtypes({"id_organisation": int})
@dc.HasNoNans(["id_organisation", "address_text", "address_rank"])
def process_address(df: pd.DataFrame) -> pd.DataFrame:
    """ Process address table """
    postcodes = postcode_extractor(df.address_text)
    low_quality = postcodes.isnull()
    return df.assign(postcode=postcodes.str.upper(), low_quality=low_quality)


# @dc.HasDtypes({"id_organisation": int})
# @dc.HasNoNans(["id_organisation", "url", "snippet"])
def process_notice(df: pd.DataFrame, date: datetime.date) -> pd.DataFrame:
    """ Process COVID notice table """
    df = df.assign(
        notice_id=lambda x: x.index.astype(str) + "-" + date.strftime("%m/%Y"),
        date=date,
    )

    notice = df[["notice_id", "id_organisation", "url", "snippet", "date"]].assign(
        low_quality=lambda x: x.isnull().sum(axis=1)
    )

    # Generate mapping between orgs and terms
    term = (
        df.set_index("notice_id")
        .matched_terms.str.split(";")
        .explode()
        .reset_index()
        .assign(low_quality=lambda x: x.isnull().sum(axis=1))
    )

    return notice, term


@dc.HasNoNans(["id_organisation", "sector_rank", "sector_name"])
@dc.WithinRange({"sector_rank": (1, 7)})
@dc.HasDtypes({"sector_rank": int, "id_organisation": int})
def process_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """ Process sectors """
    return df.assign(
        sector_name=lambda x: x.sector_name.str.lower().str.replace(" ", "_")
    )
