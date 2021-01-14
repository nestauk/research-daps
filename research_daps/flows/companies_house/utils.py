"""
Companies House data processing
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


COLUMN_MAPPINGS = {
    "CompanyName": "company_name",
    " CompanyNumber": "company_number",
    "URI": "uri",
    "RegAddress.CareOf": "address_careof",
    "RegAddress.POBox": "address_pobox",
    "RegAddress.AddressLine1": "address_line1",
    " RegAddress.AddressLine2": "address_line2",
    "RegAddress.PostCode": "address_postcode",
    "RegAddress.PostTown": "address_town",
    "RegAddress.County": "address_county",
    "RegAddress.Country": "address_country",
    "CompanyCategory": "company_category",
    "CompanyStatus": "company_status",
    "CountryOfOrigin": "country_of_origin",
    "DissolutionDate": "dissolution_date",
    "IncorporationDate": "incorporation_date",
    "SICCode.SicText_1": "sic_1",
    "SICCode.SicText_2": "sic_2",
    "SICCode.SicText_3": "sic_3",
    "SICCode.SicText_4": "sic_4",
    "PreviousName_1.CONDATE": "previous1_date",
    " PreviousName_1.CompanyName": "previous1_name",
    " PreviousName_2.CONDATE": "previous2_date",
    " PreviousName_2.CompanyName": "previous2_name",
    "PreviousName_3.CONDATE": "previous3_date",
    " PreviousName_3.CompanyName": "previous3_name",
    "PreviousName_4.CONDATE": "previous4_date",
    " PreviousName_4.CompanyName": "previous4_name",
    "PreviousName_5.CONDATE": "previous5_date",
    " PreviousName_5.CompanyName": "previous5_name",
    "PreviousName_6.CONDATE": "previous6_date",
    " PreviousName_6.CompanyName": "previous6_name",
    "PreviousName_7.CONDATE": "previous7_date",
    " PreviousName_7.CompanyName": "previous7_name",
    "PreviousName_8.CONDATE": "previous8_date",
    " PreviousName_8.CompanyName": "previous8_name",
    "PreviousName_9.CONDATE": "previous9_date",
    " PreviousName_9.CompanyName": "previous9_name",
    "PreviousName_10.CONDATE": "previous10_date",
    " PreviousName_10.CompanyName": "previous10_name",
}


def process_organisations(ch: pd.DataFrame) -> pd.DataFrame:
    """"""
    return ch[
        [
            "company_number",
            "company_name",
            "company_category",
            "company_status",
            "country_of_origin",
            "dissolution_date",
            "incorporation_date",
            "uri",
        ]
    ]


def process_address(ch: pd.DataFrame):  # , nspl: pd.DataFrame) -> pd.DataFrame:
    """ """
    return (
        ch[
            [
                "company_number",
                "address_careof",
                "address_pobox",
                "address_line1",
                "address_line2",
                "address_town",
                "address_county",
                "address_country",
                "address_postcode",
            ]
        ].rename(columns=lambda x: x.replace("address_", ""))
        # .merge(nspl, on="postcode")
    )


def process_sectors(ch: pd.DataFrame) -> pd.DataFrame:
    """ """
    return (
        ch[["company_number", "sic_1", "sic_2", "sic_3", "sic_4"]]
        .melt(
            id_vars=["company_number"],
            value_vars=["sic_1", "sic_2", "sic_3", "sic_4"],
            var_name="rank",
            value_name="SIC5_full",
        )
        .assign(
            rank=lambda x: x["rank"].str.slice(-1).astype(int),
            SIC5_code=lambda x: x["SIC5_full"].str.extract(r"([0-9]*) -"),
            SIC4_code=lambda x: x["SIC5_code"].str.slice(0, 4),
        )
        .dropna(subset=["SIC5_code"])
    )


def process_names(ch: pd.DataFrame) -> pd.DataFrame:
    """ """
    return (
        ch[
            [
                "company_number",
                "company_name",
                "previous1_date",
                "previous1_name",
                "previous2_date",
                "previous2_name",
                "previous3_date",
                "previous3_name",
                "previous4_date",
                "previous4_name",
                "previous5_date",
                "previous5_name",
                "previous6_date",
                "previous6_name",
                "previous7_date",
                "previous7_name",
                "previous8_date",
                "previous8_name",
                "previous9_date",
                "previous9_name",
                "previous10_date",
                "previous10_name",
            ]
        ]
        .pipe(_double_melt)
        .dropna(subset=["company_name"])
    )


def _double_melt(df: pd.DataFrame) -> pd.DataFrame:
    """ Melt previous dates and names of companies and join back together """
    var_name = "name_age_index"
    df_melt_name = (
        df.rename(columns={"company_name": "previous0_name"})
        .melt(
            id_vars=["company_number"],
            value_vars=[f"previous{i}_name" for i in range(0, 11)],
            var_name=var_name,
            value_name="company_name",
        )
        .dropna()
        .assign(**{var_name: lambda x: x[var_name].str.extract(r"([0-9]*)_")})
    )
    df_melt_date = (
        df.melt(
            id_vars=["company_number"],
            value_vars=[f"previous{i}_date" for i in range(1, 11)],
            var_name=var_name,
            value_name="change_date",
        )
        .dropna()
        .assign(**{var_name: lambda x: x[var_name].str.extract(r"([0-9]*)_")})
    )
    return df_melt_name.merge(df_melt_date, on=("company_number", var_name), how="left")
