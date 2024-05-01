"""
Spatial join the hh, person, and trip survey results file to SF CHAMP MAZs

The results are saved in the parsed/01-"taz_spatial_join subdirectory,
with the following columns added:
    hh.csv: home_{maz, taz}
    person.csv: {work, school}_{maz, taz}
    trip.csv: {o, d}_{maz, taz}
"""

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd


def sjoin_maz(df: pd.DataFrame, maz: gpd.GeoDataFrame, var_prefix: str):
    return (
        gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                df[f"{var_prefix}_lon"], df[f"{var_prefix}_lat"]
            ),
            crs=survey_crs,
        )
        .sjoin(maz, how="left", predicate="within")
        # column index_right was generated from the sjoin
        .drop(columns=["geometry", "index_right"])
        .rename(columns={"MAZID": f"{var_prefix}_maz", "TAZ": f"{var_prefix}_taz"})
        # casting to int doesn't work since there's NaNs:
        # .astype({f"{var_prefix}_maz": int, f"{var_prefix}_taz": int})
    )


if __name__ == "__main__":
    survey_data_dir = Path(
        r"Q:\Data\Surveys\HouseholdSurveys\MTC-SFCTA2022\Processed_20240329"
    )
    taz_spatial_join_dir = survey_data_dir / "01-taz_spatial_join"
    os.makedirs(taz_spatial_join_dir, exist_ok=True)
    maz_filepath = r"Q:\GIS\Model\MAZ\MAZ40051.shp"
    survey_crs = "EPSG:4326"

    maz = gpd.read_file(maz_filepath)[["MAZID", "TAZ", "geometry"]].to_crs(survey_crs)
    hh = pd.read_csv(survey_data_dir / "hh.csv")
    person = pd.read_csv(survey_data_dir / "person.csv")
    trip = pd.read_csv(survey_data_dir / "trip.csv")

    hh_taz_join = sjoin_maz(hh, maz, "home")
    person_taz_join = sjoin_maz(sjoin_maz(person, maz, "work"), maz, "school")
    trip_taz_join = sjoin_maz(sjoin_maz(trip, maz, "o"), maz, "d")
    hh_taz_join.to_csv(taz_spatial_join_dir / "hh-taz_spatial_join.csv")
    person_taz_join.to_csv(taz_spatial_join_dir / "person-taz_spatial_join.csv")
    trip_taz_join.to_csv(taz_spatial_join_dir / "trip-taz_spatial_join.csv")
