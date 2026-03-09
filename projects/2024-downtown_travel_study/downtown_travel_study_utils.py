"""copied and modified from 2022/04b-summary_notebooks/utils.py as needed"""

from collections import OrderedDict

import polars as pl
import polars.selectors as cs

northeast_core_analysis_neighborhoods = {
    "Financial District/South Beach",
    "Mission Bay",
    "South of Market",
    "Tenderloin",
    "Nob Hill",
    "Chinatown",
    "North Beach",
    "Russian Hill",
}


hh_income_cat_2022_dict = OrderedDict(  # for sorting order
    [
        ("Under $25,000", "000-25k"),
        ("$25,000-$49,999", "025-50k"),
        ("$50,000-$74,999", "050-75k"),
        ("$75,000-$99,999", "075-100k"),
        ("$100,000-$199,999", "100-200k"),
        ("$200,000 or more", "200k+"),
        ("995", "missing response"),  # N/A for imputed
        ("999", "prefer not to answer"),  # N/A for imputed
    ]
)


def load_hh_2018(hh_taz_spatial_join_filepath):
    """load and parse raw hh CSV

    modified from
    - 2022/02-reformat/a-reformat.py reformat_hh()
    - 2022/04b-summary_notebooks/utils.py load_hh_raw()

    - income: use the income_broad categories, rather than the mid-point of the
      income_detailed and income_followup categories (which reformat_hh() uses instead)
    """
    hh_out_cols = [
        "hhno",
        "hhsize",
        "hhvehs",
        # from person file in 2018, unless parsing 2022 data with 2019_reformat code
        # "hh_income_cat",
        # "hownrent",  # from person file in 2022
        # "hrestype",  # from person file in 2022
        # "hhparcel",
        "hhtaz",
        # "hxcord",
        # "hycord",
        "home_county",
    ]
    hh = pl.read_csv(hh_taz_spatial_join_filepath).rename(
        {
            "hh_id": "hhno",
            # new 2018 results (Processing_20211018/in_2022_format/v01) and
            # 2022's v00b/reformat_2019* directly parses into the col name "hhtaz"
            # "home_taz": "hhtaz"
            # "home_maz": "hhparcel",
            # "home_lon": "hxcord",
            # "home_lat": "hycord",
            "num_people": "hhsize",
            "num_vehicles": "hhvehs",
        }
    )
    if "hhtaz" not in hh.columns:
        raise RuntimeError
    if "home_county" in hh.columns:
        home_county_col = "home_county"
    else:  # for 2018's Processing_20211018/in_2022_format/v01
        home_county_col = "home_county_fips"
    if "income_imputed" in hh.collect_schema().names():
        # income_imputed is in person table in 2018
        # but he 2022 (parsed with 2019_reformat) results still has income in hh since
        # this pulls from the raw / spatial_join tables
        hh = hh.with_columns(
            hh_income_cat=pl.col("income_imputed").replace(hh_income_cat_2022_dict)
        )
        hh_out_cols.append("hh_income_cat")
    return (
        hh.with_columns(home_county=(pl.col(home_county_col) % 1000))
        # .join(person, on="hhno", how="left")
        .select(hh_out_cols)
        .sort(by="hhno")
    )


def load_hh_2022(hh_taz_spatial_join_filepath):
    """load and parse raw hh CSV

    modified from
    - 2022/02-reformat/a-reformat.py reformat_hh()
    - 2022/04b-summary_notebooks/utils.py load_hh_raw()

    - income: use the income_broad categories, rather than the mid-point of the
      income_detailed and income_followup categories (which reformat_hh() uses instead)
    """
    hh_out_cols = [
        "hhno",
        "hhsize",
        "hhvehs",
        "hh_income_cat",
        # "hownrent",  # from person file
        # "hrestype",  # from person file
        "hhparcel",
        "hhtaz",
        "hxcord",
        "hycord",
        "home_county",
    ]
    return (
        pl.read_csv(hh_taz_spatial_join_filepath)
        .rename(
            {
                "hh_id": "hhno",
                "home_maz": "hhparcel",
                "home_taz": "hhtaz",
                "home_lon": "hxcord",
                "home_lat": "hycord",
                "num_people": "hhsize",
                "num_vehicles": "hhvehs",
            }
        )
        .with_columns(
            # the imputed income categories state the dollar amounts directly
            hh_income_cat=pl.col("income_imputed").replace(hh_income_cat_2022_dict),
            home_county=(pl.col("home_county") % 1000),
        )
        # .join(person, on="hhno", how="left")
        .select(hh_out_cols)
        .sort(by="hhno")
    )


def _taz_cols_to_analysis_neighborhood(trips, taz_analysis_neighborhoods):
    return trips.with_columns(
        cs.ends_with("taz")
        .replace(
            taz_analysis_neighborhoods["TAZ"].to_list(),
            taz_analysis_neighborhoods["analysis_neighborhood"].to_list(),
            default=None,
        )
        .name.map(
            lambda c: c.removesuffix("taz").removesuffix("_") + "_analysis_neighborhood"
        )
    )


def _analysis_neighborhood_cols_to_geog(trips):
    return trips.with_columns(
        cs.ends_with("_analysis_neighborhood")
        .is_in(northeast_core_analysis_neighborhoods)
        .replace_strict({True: "1. northeast core", False: "2. rest of SF"})
        .fill_null("3. rest of Bay Area")
        # .cast(pl.Enum(["northeast core", "rest of SF", "rest of Bay Area"]))
        .name.map(lambda c: c.removesuffix("analysis_neighborhood") + "geog")
    )


def taz_cols_to_county(trips, taz_gdf):
    return trips.with_columns(
        cs.ends_with("taz")
        .replace(
            taz_gdf["TAZ"].to_list(),
            taz_gdf["COUNTY"].to_list(),
            default=None,
        )
        .name.map(lambda c: c.removesuffix("taz").removesuffix("_") + "_county")
    )


def parse_geog(trips, taz_analysis_neighborhoods, county=False):
    return _analysis_neighborhood_cols_to_geog(
        _taz_cols_to_analysis_neighborhood(
            trips,
            taz_analysis_neighborhoods,
        )
    )
