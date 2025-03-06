"""
copy all CSV files from raw_dir to 00-preprocess dir with minor changes:
trip: add columns: depart_time, arrive_time
location: add person_id from trip table
"""

import argparse
import tomllib
from pathlib import Path

import pandas as pd


def preprocess(config):
    raw_dir = Path(config["raw"]["dir"])
    preprocess_dir = Path(config["00-preprocess"]["dir"])
    preprocess_dir.mkdir(exist_ok=True)

    for d in [
        "01-taz_spatial_join",
        "02-reformat",
        "03a-tour_extract",
        "03b-assign_day",
        "04a-merge_skims",
        "04b-adjust_weights",
    ]:
        Path(config[d]["dir"]).mkdir(exist_ok=True, parents=True)

    primary_respondent = config["00-preprocess"]["primary_respondent_only"]
    rmove = config["00-preprocess"]["rmove_only"]
    completion_basis = config["00-preprocess"]["completion_basis"]

    # copying the unchanged files too for ease of use / easy backward compatibility;
    # not sure for logical clarity if we should copy files that are not changed
    day = pd.read_csv(raw_dir / config["day_filename"])
    hh = pd.read_csv(raw_dir / config["hh_filename"])
    person = pd.read_csv(raw_dir / config["person_filename"])
    vehicle = pd.read_csv(raw_dir / config["vehicle_filename"])
    trip = pd.read_csv(raw_dir / config["trip_filename"])
    location = pd.read_csv(raw_dir / config["location_filename"])

    print("      hh: {:10d}".format(len(hh)))
    print("  person: {:10d}".format(len(person)))
    print("     day: {:10d}".format(len(day)))
    print("    trip: {:10d}".format(len(trip)))
    print(" vehicle: {:10d}".format(len(vehicle)))
    print("location: {:10d}".format(len(location)))

    hh, person, day, trip, location = filter(
        hh, person, day, trip, location, primary_respondent, rmove
    )

    print("      hh: {:10d}".format(len(hh)))
    print("  person: {:10d}".format(len(person)))
    print("     day: {:10d}".format(len(day)))
    print("    trip: {:10d}".format(len(trip)))
    print(" vehicle: {:10d}".format(len(vehicle)))
    print("location: {:10d}".format(len(location)))

    day = preprocess_day(day)
    hh = preprocess_hh(hh, person)
    person = preprocess_person(person, day, completion_basis)
    trip = preprocess_trip(trip)

    print("      hh: {:10d}".format(len(hh)))
    print("  person: {:10d}".format(len(person)))
    print("     day: {:10d}".format(len(day)))
    print("    trip: {:10d}".format(len(trip)))
    print(" vehicle: {:10d}".format(len(vehicle)))
    print("location: {:10d}".format(len(location)))

    day.to_csv(preprocess_dir / config["day_filename"], index=False)
    hh.to_csv(preprocess_dir / config["hh_filename"], index=False)
    person.to_csv(preprocess_dir / config["person_filename"], index=False)
    trip.to_csv(preprocess_dir / config["trip_filename"], index=False)
    vehicle.to_csv(preprocess_dir / config["vehicle_filename"], index=False)
    location.to_csv(preprocess_dir / config["location_filename"], index=False)

    return


def filter(hh, person, day, trip, location, primary_respondent, rmove):
    condition = pd.Series(index=person.index, data=True)
    if primary_respondent:
        condition = condition & person["relationship"].eq(0)
    if rmove:
        condition = condition & person["diary_platform"].eq("rmove")

    person = person.loc[condition]
    hh = pd.merge(hh, person[["hh_id"]].drop_duplicates())
    day = pd.merge(day, person[["person_id"]])
    trip = pd.merge(trip, person[["person_id"]])

    location = pd.merge(location, trip[["trip_id", "person_id"]])
    return hh, person, day, trip, location


def preprocess_hh(hh, person):
    hh_rename = {
        "residence_rent_own": "rent_own",
        "residence_type": "res_type",
    }
    res_type_recode = {
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 4,
        6: 5,
        7: 7,
        9: 6,
        995: 995,
        997: 997,
    }
    hhinc_text_recode = {
        "Under $25,000": 1,
        "$25,000-$49,999": 2,
        "$50,000-$74,999": 3,
        "$75,000-$99,999": 4,
        "$100,000-$199,999": 5,
        "$200,000 or more": 6,
    }
    hhinc_nrel_recode = {
        "0_24999": 1,
        "25000_49999": 2,
        "50000_74999": 3,
        "75000_99999": 4,
        "100000_199999": 5,
        "200000_plus": 6,
    }

    # use primary respondent (relationship == 0) to determine rent/own & residence type
    # in 2019 this info was collected for the household, not the person
    hh = pd.merge(
        hh,
        person.loc[
            person["relationship"].eq(0),
            ["hh_id", "person_num", "residence_rent_own", "residence_type"],
        ],
    )
    hh.rename(columns=hh_rename, inplace=True)
    hh.loc[hh["rent_own"].eq(4), "rent_own"] = 997
    hh["reported_home_lon"] = hh["home_lon"]
    hh["reported_home_lat"] = hh["home_lat"]
    hh["wt_alladult_wkday"] = hh["hh_weight"]
    hh["wt_alladult_7day"] = hh["hh_weight"]
    hh["res_type"] = hh["res_type"].map(lambda x: res_type_recode[x])
    hh["hhinc_imputed"] = hh["income_imputed"].map(lambda x: hhinc_text_recode[x])
    hh["hhinc_nonrel_imputed"] = hh["nonrel_income_imputed"].map(
        lambda x: hhinc_nrel_recode[x]
    )
    return hh


def preprocess_person(person, day, completion_basis="person"):
    student_recode = {2: 0, 0: 1, 1: 2, 3: 2, 4: 1, 995: 995, 997: 997, 999: 999}

    if completion_basis == "person":
        v = "is_complete"
    elif completion_basis == "household":
        v = "hh_day_complete"
    else:
        raise Exception('`completion_basis` must be either "person" or "household"')

    comp = (
        day.pivot_table(
            index="person_id", columns="travel_date_dow", values=v, aggfunc="sum"
        )
        .fillna(0)
        .rename(
            columns={
                1: "mon_complete",
                2: "tue_complete",
                3: "wed_complete",
                4: "thu_complete",
                5: "fri_complete",
                6: "sat_complete",
                7: "sun_complete",
            }
        )
        .reset_index()
    )
    comp["nwkdaywts_complete"] = comp.loc[:, "mon_complete":"thu_complete"].sum(axis=1)
    comp["n7daywts_complete"] = comp.loc[:, "mon_complete":"sun_complete"].sum(axis=1)
    person = pd.merge(person, comp, how="left", on="person_id")

    person["hours_work"] = -1
    person.loc[person["employment"].isin([1, 3, 8]), "hours_work"] = 2
    person.loc[person["employment"].isin([2, 7]), "hours_work"] = 8
    person.loc[person["age"].eq(11), "age"] = 10
    person.loc[person["employment"].eq(5), "employment"] = 6
    person.loc[person["employment"].eq(8), "employment"] = 1
    person["student"] = person["student"].map(lambda x: student_recode[x])
    person["work_county_fips"] = person["work_county"] - 6000
    person["school_county_fips"] = person["school_county"] - 6000
    person["wt_alladult_wkday"] = person["person_weight"]
    person["wt_alladult_7day"] = person["person_weight"]
    person.loc[person["job_type"].eq(5), "job_type"] = 1
    return person


def preprocess_day(day):
    day_rename = {"travel_dow": "travel_date_dow"}
    day = day.rename(columns=day_rename)
    return day


def preprocess_trip(trip):
    purpose_category_recode = {
        -1: -1,
        1: 1,
        2: 2,
        3: 3,
        4: 4,
        5: 4,
        6: 5,
        7: 6,
        8: 7,
        9: 8,
        10: 9,
        11: 10,
        12: 11,
        13: 12,
        995: 995,
        997: 997,
        999: 999,
    }

    mode_type_recode = {
        -1: -1,
        1: 1,
        2: 2,
        3: 11,
        4: 12,
        5: 4,
        6: 9,
        7: 7,
        8: 3,
        9: 10,
        10: 6,
        11: 8,
        12: 5,
        13: 5,
        14: 13,
        995: 995,
        997: 997,
        999: 999,
    }

    accegr_recode = {
        1: 1,
        2: 2,
        3: 8,
        4: 3,
        5: 8,
        6: 5,
        7: 6,
        8: 7,
        9: 6,
        10: 7,
        995: 995,
        997: 997,
        999: 999,
    }

    trip_rename = {"travel_dow": "travel_date_dow", "depart_seconds": "depart_second"}
    print("trip raw len:", len(trip))
    if "depart_seconds" in trip.columns:
        trip.rename(columns={"depart_seconds": "depart_second"}, inplace=True)

    trip = trip.rename(columns=trip_rename)
    trip["o_purpose_category_imputed"] = trip["o_purpose_category"].map(
        lambda x: purpose_category_recode[x]
    )
    trip["d_purpose_category_imputed"] = trip["d_purpose_category"].map(
        lambda x: purpose_category_recode[x]
    )
    trip["o_county_fips"] = trip["o_county"] - 6000
    trip["d_county_fips"] = trip["d_county"] - 6000
    trip["mode_type_imputed"] = trip["mode_type"].map(lambda x: mode_type_recode[x])
    trip["bus_access"] = trip["transit_access"].map(lambda x: accegr_recode[x])
    trip["bus_egress"] = trip["transit_egress"].map(lambda x: accegr_recode[x])
    trip["rail_access"] = trip["transit_access"].map(lambda x: accegr_recode[x])
    trip["rail_egress"] = trip["transit_egress"].map(lambda x: accegr_recode[x])

    # for c in ['mode_1','mode_2','mode_3','mode_4']:
    #    trip.loc[trip[c].eq(78), c] = 30
    #    trip.loc[trip[c].eq(80), c] = 30
    #    trip.loc[trip[c].eq(105), c] = 39

    for c in ["mode_1", "mode_2", "mode_3", "mode_4"]:
        trip.loc[trip[c].eq(78), c] = (
            32  # Public ferry or water taxi -> Boat/ferry/water taxi
        )
        trip.loc[trip[c].eq(80), c] = (
            32  # Other boat (e.g., kayak) -> Boat/ferry/water taxi
        )
        trip.loc[trip[c].eq(105), c] = (
            39  # Rail (e.g., train, light rail, trolley, BART, MUNI Metro) -> Light rail (Muni Metro, Santa Clara VTA)
        )
        trip.loc[trip[c].eq(53), c] = (
            39  # MUNI Metro -> Light rail (Muni Metro, Santa Clara VTA)
        )

    trip.loc[trip["driver"].eq(3), "driver"] = 1
    trip["depart_time_imputed"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["depart_hour"], x["depart_minute"], x["depart_second"]
        ),
        axis=1,
    )
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
    trip["daywt_alladult_wkday"] = trip["trip_weight"]
    trip["daywt_alladult_7day"] = trip["trip_weight"]

    trip.drop(columns=["is_transit"], inplace=True)
    return trip


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    preprocess(config)
