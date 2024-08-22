"""
minor changes to CSV files:
person: add person_id_sfcta column by concatenating hh_id and person_num
trip: add columns: depart_time, arrive_time, person_id_sfcta, trip_id_sfcta
location: add person_id, person_id_sfcta, trip_id, trip_id_sfcta from trip table
"""

import argparse
import tomllib
from pathlib import Path
from shutil import copy2

import pandas as pd


def preprocess(config):
    raw_dir = Path(config["raw_dir"])
    preprocess_dir = Path(config["00-preprocess"]["dir"])
    preprocess_dir.mkdir(exist_ok=True)

    # TODO move the construction of filenames into a utils.py
    hh_filename = f"{config["hh_filename_stem"]}.csv"
    person_filename = f"{config["person_filename_stem"]}.csv"
    day_filename = f"{config["day_filename_stem"]}.csv"
    trip_filename = f"{config["trip_filename_stem"]}.csv"
    location_filename = f"{config["location_filename_stem"]}.csv"
    vehicle_filename = f"{config["vehicle_filename_stem"]}.csv"

    _ = preprocess_person(raw_dir, preprocess_dir, person_filename)
    trip = preprocess_trip(raw_dir, preprocess_dir, trip_filename)
    _ = preprocess_location(raw_dir, preprocess_dir, location_filename, trip)

    # copying the unchanged files too for ease of use / easy backward compatibility;
    # not sure if we should copy files that are not changed
    copy2(raw_dir / hh_filename, preprocess_dir / hh_filename)
    copy2(raw_dir / day_filename, preprocess_dir / day_filename)
    copy2(raw_dir / vehicle_filename, preprocess_dir / vehicle_filename)
    return


def preprocess_person(raw_dir, preprocess_dir, person_filename):
    person = pd.read_csv(raw_dir / person_filename)
    print("persons:", len(person))
    # TODO TO VERIFY: as of 2022 data, seems like no longer needed to create
    # person_id_sfcta because person_id is exactly what this new column is
    person["person_id_sfcta"] = person.apply(
        lambda x: "{:d}{:02d}".format(x["hh_id"], x["person_num"]), axis=1
    ).astype("int64")
    print("persons:", len(person))
    person.to_csv(preprocess_dir / person_filename, index=False)
    return person


def preprocess_trip(raw_dir, preprocess_dir, trip_filename):
    trip = pd.read_csv(raw_dir / trip_filename)
    print("trips:", len(trip))

    if "depart_seconds" in trip.columns:
        trip.rename(columns={"depart_seconds": "depart_second"}, inplace=True)
    trip["depart_time"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["depart_hour"], x["depart_minute"], x["depart_second"]
        ),
        axis=1,
    )
    trip["arrive_time"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["arrive_hour"], x["arrive_minute"], x["arrive_second"]
        ),
        axis=1,
    )

    trip["person_id_sfcta"] = trip.apply(
        lambda x: "{:d}{:02d}".format(x["hh_id"], x["person_num"]), axis=1
    ).astype("int64")
    trip["trip_id_sfcta"] = trip.apply(
        lambda x: "{:d}{:03d}".format(x["person_id_sfcta"], x["trip_num"]), axis=1
    ).astype("int64")

    print("trips:", len(trip))
    trip.to_csv(preprocess_dir / trip_filename, index=False)
    return trip


def preprocess_location(raw_dir, preprocess_dir, location_filename, trip):
    location = pd.read_csv(raw_dir / location_filename)
    print("locations:", len(location))
    location = pd.merge(
        trip[["person_id", "person_id_sfcta", "trip_id", "trip_id_sfcta"]],
        location,
        on="trip_id",
        how="right",
    )
    print("locations:", len(location))
    location.to_csv(preprocess_dir / location_filename, index=False)
    return location


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    preprocess(config)
