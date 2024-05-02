"""Map survey results CSVs to Daysim format"""

import argparse
import datetime
from pathlib import Path

import polars as pl
import tomllib

OUT_SEP = " "
COUNTY_FIPS = [1, 13, 41, 55, 75, 81, 85, 95, 97]


def reformat_person(in_person_filepath, logfile):
    """Person file processing"""
    print(f"Person file processing started: {datetime.datetime.now()}")
    logfile.write(f"\nPerson file processing started: {datetime.datetime.now()}\n")

    age_dict = {
        1: 3,
        2: 10,
        3: 16,  # 16-17
        4: 21,
        5: 30,
        6: 40,
        7: 50,
        8: 60,
        9: 70,
        10: 80,
        11: 90,  # 85+
    }
    gender_dict = {
        1: 2,  # female
        2: 1,  # male
        4: 3,  # non-binary
        997: 3,  # other/self-describe
        995: 9,  # missing
        999: 9,  # prefer not to answer
    }
    student_dict = {
        0: 1,  # full-time, some/all in-person
        1: 2,  # part-time, some/all in-person
        2: 0,  # not student
        3: 2,  # part-time, remote only
        4: 1,  # full-time, remote only
        995: -1,  # missing
    }
    work_park_dict = {
        1: 0,  # free parking at work
        # ppaidpark = 1: paid parking at work
        2: 1,
        3: 1,
        4: 1,
        995: -1,  # missing
        997: -1,  # never drive to work
        998: -1,  # don't know
    }
    residence_rent_own_dict = {
        1: 1,  # own
        2: 2,  # rent
        3: 3,  # provided by military -> other
        4: 3,  # provided by family/relative/freind rent-free -> other
        997: 3,  # other
        995: 9,  # missing
        999: 9,  # prefer not to answer -> missing
    }
    residence_type_dict = {
        1: 1,  # detached house
        2: 2,  # rowhouse/townhouse -> duplex/triplex/rowhouse
        3: 3,  # duplex/triplex/quads 2-4 units -> apt/condo
        4: 3,  # apt/condos 5-49 units
        5: 3,  # apt/condos 50+ units
        6: 3,  # senior/age-restricted apt/condos
        7: 4,  # manufactured/mobile home, trailer -> mobile home, trailer
        9: 5,  # dorm, group qarters, inst housing -> dorm/rented room
        995: 9,  # missing
        997: 6,  # other
    }
    person_out_cols = [
        "hhno",
        "pno",
        "pptyp",
        "pagey",
        "pgend",
        "pwtyp",
        "pwpcl",
        "pwtaz",
        "pstyp",
        "pspcl",
        "pstaz",
        "ppaidprk",
        "pwxcord",
        "pwycord",
        "psxcord",
        "psycord",
        "pownrent",  # for joining to hh table later (usu in hh for Daysim)
        "prestype",  # for joining to hh table later (usu in hh for Daysim)
        "num_days_complete",
    ]
    if weighted:
        person_out_cols += [
            "wt_alladult_wkday",
            "wt_alladult_7day",
            "mon_complete",
            "tue_complete",
            "wed_complete",
            "thu_complete",
            "fri_complete",
            "sat_complete",
            "sun_complete",
            "num_days_complete_weekday",  # moved to the hh table in 2022
        ]
    person = (
        pl.read_csv(
            in_person_filepath,
            dtypes={
                "hh_id": int,
                "person_num": int,
                "person_id": int,
                "pgend": int,
            },
        )
        .cast(
            {
                "work_taz": int,
                "work_maz": int,
                "school_taz": int,
                "school_maz": int,
            }
        )
        .rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
                "work_lon": "pwxcord",
                "work_lat": "pwycord",
                "school_lon": "psxcord",
                "school_lat": "psycord",
                "work_taz": "pwtaz",
                "work_maz": "pwpcl",
                "school_taz": "pstaz",
                "school_maz": "pspcl",
            }
        )
        .with_columns(
            pl.col(["pwxcord", "pwycord", "psxcord", "psycord"]).fill_null(-1),
            pagey=pl.col("age").replace(age_dict),
            pgend=pl.col("gender").replace(gender_dict),
            # NOTE pstyp/student: bad logic for the 0 as default! (copied from 2019)
            # since the student var only applies to people above 16
            pstyp=pl.col("student").replace(student_dict).fill_null(0),
            # ppaidprk: paid parking at workplace?
            ppaidprk=pl.col("work_park").replace(work_park_dict),
            # pownrent & prestype: for joining to hh table (to conform to Daysim)
            pownrent=pl.col("residence_rent_own").replace(residence_rent_own_dict),
            prestype=pl.col("residence_type").replace(residence_type_dict),
        )
        .with_columns(
            pptyp=pl.when(pl.col("pagey") < 5)
            .then(pl.lit(8))  # child 0-4
            .when(pl.col("pagey") < 16)
            .then(pl.lit(7))  # child 5-15
            # only if age >= 16:
            .when(pl.col("employment") == 1)  # employed full-time
            .then(pl.lit(1))  # full-time worker
            # all cases below: if not full-time employed:
            .when(pl.col("pagey") < 18)
            .then(pl.lit(6))  # high school 16+
            .when(
                (pl.col("pagey") < 25)
                & (pl.col("school_type")).is_in([4, 7])  # home school, high school
                & (pl.col("student").is_in([0, 4]))  # full-time, in-person/remote
            )
            .then(pl.lit(6))  # high school 16+
            # logic below is for age 18-65
            .when(
                # full/part-time student, in-person & remote
                pl.col("student").is_in([0, 1, 3, 4])
            )
            .then(pl.lit(5))  # university student
            .when(pl.col("employment").is_in([2, 3]))  # part-time/self employed
            # NOTE we're counting self-employed people as part-time workers here
            .then(pl.lit(2))  # part-time worker
            # only if not full/part-time or self employed (includes employment codes
            # 7: unpaid volunteer/intern and
            # 8: employed but not currently working (e.g. leave/furlough)):
            .when(pl.col("pagey") < 65)
            .then(pl.lit(4))  # non-working age < 65
            .otherwise(pl.lit(3)),  # non-working age 65+
        )
        .with_columns(
            pwtyp=pl.when(pl.col("pptyp").is_in([1, 2]))
            .then(pl.col("pptyp"))  # direct mapping pptyp -> pwtyp for 1 or 2
            # categorize student workers as paid part-time workers
            .when(
                pl.col("pptyp").is_in([5, 6])  # student: uni or high school 16+
                # employed: full-time, part-time, self-
                & pl.col("employment").is_in([1, 2, 3])
            )
            .then(pl.lit(2))  # paid part time
            .otherwise(pl.lit(0))
        )
        .with_columns(
            # pwtaz, pstaz, pwpcl, pspcl: only keep those within Bay Area
            # p{w, s}{taz, pcl, xcord, ycord}: in previous surveys,
            # some persons are not workers/students but have school loc:
            # account for that by setting school loc to null/missing
            pwtaz=pl.when(
                pl.col(["work_county"]).is_in(COUNTY_FIPS) & pl.col("pwtyp") != 0
            )
            .then(pl.col("pwtaz"))
            .otherwise(pl.lit(-1)),
            pwpcl=pl.when(
                pl.col(["work_county"]).is_in(COUNTY_FIPS) & pl.col("pwtyp") != 0
            )
            .then(pl.col("pwpcl"))
            .otherwise(pl.lit(-1)),
            pwxcord=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwxcord"))
            .otherwise(pl.lit(-1)),
            pwycord=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwycord"))
            .otherwise(pl.lit(-1)),
            pstaz=pl.when(
                pl.col(["school_county"]).is_in(COUNTY_FIPS) & pl.col("pstyp") != 0
            )
            .then(pl.col("pstaz"))
            .otherwise(pl.lit(-1)),
            pspcl=pl.when(
                pl.col(["school_county"]).is_in(COUNTY_FIPS) & pl.col("pstyp") != 0
            )
            .then(pl.col("pspcl"))
            .otherwise(pl.lit(-1)),
            psxcord=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("psxcord"))
            .otherwise(pl.lit(-1)),
            psycord=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("psycord"))
            .otherwise(pl.lit(-1)),
        )
        .select(person_out_cols)
        .sort(by=["hhno", "pno"])
    )
    print(f"Person file processing finished: {datetime.datetime.now()}")
    logfile.write(f"Person file processing finished: {datetime.datetime.now()}\n")
    return person


def reformat_hh(in_hh_filepath, person, logfile):
    """Household file processing"""
    print(f"Household file processing started: {datetime.datetime.now()}")
    logfile.write(f"\nHousehold file processing started: {datetime.datetime.now()}\n")

    income_detailed_dict = {
        999: -1,
        1: 7500,
        2: 20000,
        3: 30000,
        4: 42500,
        5: 62500,
        6: 87500,
        7: 125000,
        8: 175000,
        9: 225000,
        10: 350000,  # 250k+
    }
    income_followup_dict = {
        999: -1,
        1: 12500,
        2: 37500,
        3: 62500,
        4: 87500,
        5: 150000,  # in 2019, this was 175000
        6: 250000,  # 200k+; in 2019, this was 350000
    }
    hh_out_cols = [
        "hhno",
        "hhsize",
        "hhvehs",
        "hhincome",
        "hownrent",
        "hrestype",
        "hhparcel",
        "hhtaz",
        "hxcord",
        "hycord",
    ]
    if weighted:
        hh_out_cols += ["wt_alladult_wkday", "wt_alladult_7day"]
    person = (
        person.select("hhno", "pownrent", "prestype")
        .group_by("hhno")
        # from spot checking the data, the first person in the houehold has the values
        # for the ownrent and restype columns; the remaining members of the houeholds
        # has the value 995 (missing)
        .first()
        .rename({"pownrent": "hownrent", "prestype": "hrestype"})
    )
    hh = (
        pl.read_csv(
            in_hh_filepath,
            dtypes={
                "hhparcel": int,
                "hhtaz": int,
                "hhincome": int,
                "hrestype": int,
            },
        )
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
            pl.col("income_detailed").replace(income_detailed_dict),
            pl.col("income_followup").replace(income_followup_dict),
        )
        .with_columns(
            # replace income_detailed with income_followup if income_detailed == -1
            # note though that income_followup could also be -1
            pl.when(pl.col("income_detailed") > 0)
            .then(pl.col("income_detailed"))
            .otherwise(pl.col("income_followup"))
            .alias("hhincome"),
        )
        .join(person, on="hhno", how="left")
        .select(hh_out_cols)
        .sort(by="hhno")
    )

    print(f"Household file processing finished: {datetime.datetime.now()}")
    logfile.write(
        f"Household file processing finished: {str(datetime.datetime.now())}\n"
    )

    return hh


def reformat_trip(in_trip_filepath, logfile):
    """Trip processing"""
    print(f"Trip file processing started: {datetime.datetime.now()}")
    logfile.write(f"\nTrip file processing started: {datetime.datetime.now()}\n")

    purpose_dict = {
        -1: -1,  # not imputable -> missing
        995: -1,  # missing -> missing
        1: 0,  # home -> home
        2: 1,  # work -> work
        3: 1,  # work-related -> work
        4: 2,  # school -> school
        5: 2,  # school related -> school
        6: 3,  # escort -> escort
        7: 5,  # shop -> shop
        8: 6,  # meal -> meal
        9: 7,  # socrec -> socrec
        10: 4,  # errand/other -> pers.bus
        11: 10,  # change mode -> change mode
        12: 11,  # overnight non-home -> other
        13: 11,  # other -> other
    }
    trip_out_cols = [
        "hhno",
        "pno",
        "tripno",
        "dow",
        "opurp",
        "dpurp",
        "opcl",
        "otaz",
        "dpcl",
        "dtaz",
        "mode",
        "path",
        "dorp",
        "deptm",
        "arrtm",
        "oxcord",
        "oycord",
        "dxcord",
        "dycord",
        "mode_type",
    ]
    if weighted:
        trip_out_cols += ["daywt_alladult_wkday", "daywt_alladult_7day"]
    trip = (
        pl.read_csv(
            in_trip_filepath,
            dtypes={"person_id": int, "opurp": int, "dpurp": int},
        )
        .cast(
            {
                "o_taz": int,
                "o_maz": int,
                "d_taz": int,
                "d_maz": int,
            }
        )
        .rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
                "o_taz": "otaz",
                "o_maz": "opcl",
                "d_taz": "dtaz",
                "d_maz": "dpcl",
                "o_lon": "oxcord",
                "o_lat": "oycord",
                "d_lon": "dxcord",
                "d_lat": "dycord",
                "trip_num": "tripno",
                "travel_dow": "dow",
            }
        )
        # retain only trips in complete person-days
        .filter(pl.col("day_is_complete") == 1)
        .with_columns(
            pl.col(["oxcord", "oycord", "dxcord", "dycord"]).fill_null(-1),
            # NOTE deptm/arrtm are NOT using standard Daysim definitions
            deptm=(pl.col("depart_hour") * 100 + pl.col("depart_minute")),
            arrtm=(pl.col("arrive_hour") * 100 + pl.col("arrive_minute")),
            # {o, d}{taz, pcl}: only keep those within Bay Area
            otaz=pl.when(pl.col(["o_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("otaz"))
            .otherwise(pl.lit(-1)),
            opcl=pl.when(pl.col(["o_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("opcl"))
            .otherwise(pl.lit(-1)),
            dtaz=pl.when(pl.col(["d_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("dtaz"))
            .otherwise(pl.lit(-1)),
            dpcl=pl.when(pl.col(["d_county"]).is_in(COUNTY_FIPS))
            .then(pl.col("dpcl"))
            .otherwise(pl.lit(-1)),
            opurp=pl.col("o_purpose_category").replace(purpose_dict),
            dpurp=pl.col("d_purpose_category").replace(purpose_dict),
            # Daysim mode:
            # 0-other 1-walk 2-bike 3-DA 4-hov2 5-hov3
            # 6-walktran 7-drivetran 8-schbus 9-tnc
            mode=pl.when(pl.col("mode_type") == 1)  # walk
            .then(pl.lit(1))
            .when(pl.col("mode_type").is_in([2, 3, 4]))  # bike, bike/scooter-share
            .then(pl.lit(2))
            .when(pl.col("mode_type").is_in([8, 9]))  # car, carshare
            .then(
                pl.when(pl.col("num_travelers") == 1)
                .then(pl.lit(3))
                .when(pl.col("num_travelers") == 2)
                .then(pl.lit(4))
                .when(pl.col("num_travelers") > 2)
                .then(pl.lit(5))
                # do we need to check to see num_travelers always > 0?
            )
            .when(pl.col("mode_type").is_in([5, 6]))  # taxi, tnc
            .then(pl.lit(9))
            .when(pl.col("mode_type").is_in([5, 6]))  # taxi, tnc
            .then(pl.lit(9))
            .when(pl.col("mode_type") == 10)  # school bus
            .then(pl.lit(8))
            .when(pl.col("mode_type") == 11)  # shuttle/vanpool
            .then(pl.lit(5))  # make shuttle/vanpool HOV3+
            .when(
                pl.col("mode_type").is_in([12, 13])  # ferry, transit
                | (
                    (pl.col("mode_type") == 14)  # long distance pax
                    & (pl.col("mode_1") == 41)  # intercity/commuter rail
                )
            )
            .then(
                pl.when(
                    # uber/lyft, taxi, car service; drove/parked/dropped off in veh
                    pl.col("transit_access").is_in([6, 7, 8, 9])
                    | pl.col("transit_egress").is_in([6, 7, 8, 9])
                )
                .then(pl.lit(7))  # drivetran
                .otherwise(pl.lit(6))  # walktran
            )
            .otherwise(pl.lit(0)),
        )
        .with_columns(
            # Daysim pathtype
            # 0-none 1-fullnetwork 2-no-toll network 3-bus 4-lrt
            # 5-premium 6-BART 7-ferry
            # (NOTE Daysim userguide says 5 = premium bus and 6 = commuter rail, but I
            # kept the 2019 ligic that kept 6 as only BART and grouped commuter rail
            # under 5)
            path=pl.when(pl.col("mode_type") == 8)  # car
            .then(pl.lit(1))  # full network
            .when(pl.col("mode").is_in([6, 7]))  # Daysim mode: transit
            .then(
                pl.when(
                    pl.any_horizontal(
                        # ferry / water taxi
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4") == 78
                    )
                )
                .then(pl.lit(7))  # ferry
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4") == 30  # BART
                    )
                )
                .then(pl.lit(6))  # see notes above
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in(
                            # intercity rail, other rail, express/transbay bus
                            [41, 42, 55]
                        )
                    )
                )
                .then(pl.lit(5))  # see notes above
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in(
                            [
                                # Rail (e.g. train, light rail, trolley, BART, MUNI
                                # Metro) [even though BART has its own mode code 30]
                                105,
                                # cable car, streetcar
                                68,
                            ]
                        )
                    )
                )
                .then(pl.lit(4))  # LRT
                .when(
                    pl.any_horizontal(
                        pl.col("mode_1", "mode_2", "mode_3", "mode_4") == 30  # BART
                    )
                )
                .then(pl.lit(6))  # see notes above
                .otherwise(pl.lit(3))  # bus
            )
            # NOTE `otherwise` includes taxi, shuttle/vanpool, TNC, carshare, schoolbus
            .otherwise(pl.lit(0)),  # none
            dorp=pl.when(pl.col("mode").is_in([3, 4, 5]))  # sov, hov 2/3+
            .then(
                pl.when(pl.col("driver") == 1)  # driver
                .then(pl.lit(1))  # driver
                .when(pl.col("driver") == 2)  # passenger
                .then(pl.lit(2))  # passenger
                # driver == 3: both driver and pax (switched drivers during trip)
                # -> NOTE also code as missing
                .otherwise(pl.lit(9))  # missing (for all remaining car trips)
            )
            .when(pl.col("mode") == 9)  # TNC
            .then(
                pl.when(pl.col("num_travelers") == 1)
                .then(pl.lit(11))
                .when(pl.col("num_travelers") == 2)
                .then(pl.lit(12))
                .when(pl.col("num_travelers") > 2)
                .then(pl.lit(13))
            )
            .otherwise(pl.lit(3)),  # N/A
        )
        .select(trip_out_cols)
        .sort(by=["hhno", "pno", "tripno"])
    )

    print(f"Trip file processing finished: {datetime.datetime.now()}")
    logfile.write(f"Trip file processing finished: {datetime.datetime.now()}\n")
    return trip


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    weighted = False  # TODO add as argparse option; only received unweighted so far
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)

    logfilename = "a-reformat.log"
    logfile = open(logfilename, "w")
    logfile.write(
        "Reformat survey program started: " + str(datetime.datetime.now()) + "\n"
    )

    in_dir = Path(config["input"]["dir"])
    in_hh_filepath = in_dir / config["input"]["hh_filename"]
    in_person_filepath = in_dir / config["input"]["person_filename"]
    in_trip_filepath = in_dir / config["input"]["trip_filename"]

    out_dir = Path(config["output"]["dir"])
    out_dir.mkdir(exist_ok=True)
    out_hh_filepath = out_dir / config["output"]["hh_filename"]
    out_person_filepath = out_dir / config["output"]["person_filename"]
    out_trip_filepath = out_dir / config["output"]["trip_filename"]

    person = reformat_person(in_person_filepath, logfile)
    person.write_csv(out_person_filepath, separator=OUT_SEP)
    hh = reformat_hh(in_hh_filepath, person, logfile)
    hh.write_csv(out_hh_filepath, separator=OUT_SEP)
    trip = reformat_trip(in_trip_filepath, logfile)
    trip.write_csv(out_trip_filepath, separator=OUT_SEP)
    print(f"2022/02-reformat/a-reformat.py done: {datetime.datetime.now()}")
    logfile.write("\n2022/02-reformat/a-reformat.py done: {datetime.datetime.now()}\n")
    logfile.close()
