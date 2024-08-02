from pathlib import Path

survey_processed_dir = Path(
    "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2022/Processed_20240329"
)
hh_raw_filepath = survey_processed_dir / "hh.csv"
person_raw_filepath = survey_processed_dir / "person.csv"
trip_raw_filepath = survey_processed_dir / "trip.csv"
taz_spatial_join_dir = survey_processed_dir / "01-taz_spatial_join"
reformat_dir = survey_processed_dir / "02-reformat"
person_reformat_filepath = reformat_dir / "person-reformat.csv"
tour_extract_wkday_dir = survey_processed_dir / "03-tour_extract" / "wt_wkday"
tour_extract_allwk_dir = survey_processed_dir / "03-tour_extract" / "wt_7day"
out_dir = survey_processed_dir / "04b-summary_notebooks"

sup_dist_filepath = r"Q:\GIS\Model\TAZ\SFCTA_TAZ\TAZ_SUPDIST\sftaz_wSupDist_Manual.csv"
county_filepath = r"Q:\GIS\Model\TAZ\SFCTA_TAZ\TAZ2454_clean.csv"
