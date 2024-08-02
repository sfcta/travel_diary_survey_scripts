from collections import OrderedDict
from pathlib import Path

import pandas as pd
import polars as pl

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
out_dir.mkdir(exist_ok=True)

sup_dist = pd.read_csv(
    r"Q:\GIS\Model\TAZ\SFCTA_TAZ\TAZ_SUPDIST\sftaz_wSupDist_Manual.csv"
)
sup_dist = sup_dist[["TAZ", "DIST_NUM"]]

county_df = pd.read_csv(r"Q:\GIS\Model\TAZ\SFCTA_TAZ\TAZ2454_clean.csv")
county_df = county_df[["TAZ", "COUNTY"]]


raceeth_dict = OrderedDict(  # same order needed for keys/values in df_to_excel_col_dict
    [
        ("hispanic", "Hispaic"),
        ("multi", "multi-racial"),
        ("black", "African American or Black"),
        ("aiak", "American Indian or Alaska Native"),
        ("asian", "Asian"),
        ("pacific_islander", "Native Hawaiian or other Pacific Islander"),
        ("white", "White"),
        ("other", "other race"),
    ]
)


tnc_replacement_dict = OrderedDict(
    [  # OrderedDict: same order needed for keys/values in df_to_excel_col_dict
        ("multi", "0. multiple TNC replacement selected"),
        ("taxi", "1. taxi"),
        ("drive", "2. drive my own car"),
        ("shared_ride", "3. ride with family/friend/coworker"),
        ("BART", "4. BART"),
        ("transit_non_BART", "5. other transit (not BART)"),
        ("walkbike", "6. walk/bike"),
        ("other_destination", "7. travel to different place instead"),
        ("no_trip", "8. would NOT make the trip at all"),
        ("other", "9. other"),
    ]
)


def load_hh_raw(hh_raw_filepath=hh_raw_filepath, income=False, home_county=False):
    """load raw hh CSV to get income

    easier to use the raw CSV to get the recoded broad income categories,
    rather than deal with the middle-of-the-category income values from 2a reformat
    """
    column_rename_dict = {"hh_id": "hhno"}
    if income:
        column_rename_dict |= {"income_broad": "hh_income_broad_cat"}
    columns = list(column_rename_dict.keys())
    if home_county:
        columns = columns + ["home_county"]
    hh = pl.read_csv(hh_raw_filepath, columns=columns).rename(column_rename_dict)
    if home_county:
        hh = hh.with_columns(home_county=(pl.col("home_county") % 1000))
    return hh


def load_person_demographics(
    person_raw_filepath=person_raw_filepath,
    person_reformat_filepath=person_reformat_filepath,
    # raceeth_dict=raceeth_dict,
):
    raceeth_cols = [f"ethnicity_{i}" for i in [1, 2, 3, 4, 997, 999]] + [
        f"race_{i}" for i in [1, 2, 3, 4, 5, 997, 999]
    ]
    person_raw = (
        parse_raceeth(
            pl.read_csv(
                person_raw_filepath,
                columns=["hh_id", "person_num"] + raceeth_cols,
            ),
            raceeth_cols,
            # raceeth_dict=raceeth_dict,
        )
        .rename(
            {
                "hh_id": "hhno",
                "person_num": "pno",
            }
        )
        .select("hhno", "pno", "raceeth")
    )
    person_reformat = pl.read_csv(
        person_reformat_filepath, columns=["hhno", "pno", "pgend", "pagey"]
    )
    return person_reformat.join(person_raw, on=["hhno", "pno"], how="inner")


def load_person_assign_day(
    person_assign_day_filepath,
    person_raw_filepath=person_raw_filepath,
    person_reformat_filepath=person_reformat_filepath,
    demographics=False,
    raceeth_dict=raceeth_dict,
):
    person = (
        pl.read_csv(person_assign_day_filepath, columns=["hhno", "pno", "psexpfac"])
        .filter(pl.col("psexpfac") > 0)
        .with_columns(
            count=pl.lit(1)  # NOTE unsure if needed, copied over from 2019 code
        )
    )
    if demographics:
        person = person.join(
            load_person_demographics(
                person_raw_filepath=person_raw_filepath,
                person_reformat_filepath=person_reformat_filepath,
                raceeth_dict=raceeth_dict,
            ),
            on=["hhno", "pno"],
            how="left",
        )
    return person


def parse_raceeth(person_raw, raceeth_cols):  # , raceeth_dict=raceeth_dict):
    """parse race/ethnicity into a single column"""
    # TODO figure out setting up raceeth as a pl.Enum column
    # raceeth_enum_dtype = pl.Enum(raceeth_dict.keys())
    return (
        person_raw.with_columns(
            pl.col(raceeth_cols).replace({995: None}),  # missing
        )
        .with_columns(
            race_multi=(
                pl.sum_horizontal([f"race_{i}" for i in [1, 2, 3, 4, 5, 997]]) > 1
            ),
        )
        .with_columns(pl.col(raceeth_cols).cast(bool))
        .with_columns(
            hispanic=(
                pl.col("ethnicity_2")
                | pl.col("ethnicity_3")
                | pl.col("ethnicity_4")
                | pl.col("ethnicity_997")
            ),
        )
        .rename({"ethnicity_1": "not_hispanic"})
        .with_columns(
            raceeth=pl.when(pl.col("not_hispanic") & pl.col("hispanic"))
            # sanity check: people claiming both to be hispanic and non-hispanic
            .then(pl.lit(None))  # alternatively: could maybe just use the race
            .when(pl.col("hispanic"))
            .then(pl.lit("hispanic"))
            # all cases below are non-hispanic
            .when(pl.col("race_multi"))
            .then(pl.lit("multi"))
            # race_1: African American or Black
            .when(pl.col("race_1"))
            .then(pl.lit("black"))
            # race_2: American Indian or Alaska Native
            .when(pl.col("race_2"))
            .then(pl.lit("aiak"))
            # race_3: Asian
            .when(pl.col("race_3"))
            .then(pl.lit("asian"))
            # race_4: Native Hawaiian or other Pacific Islander
            .when(pl.col("race_4"))
            .then(pl.lit("pacific_islander"))
            # race_5: White
            .when(pl.col("race_5"))
            .then(pl.lit("white"))
            # race_6: other race
            .when(pl.col("race_997"))
            .then(pl.lit("other"))
            .otherwise(
                pl.lit(None)  # includes race/ethnicity 999 (prefer not to answer)
            )
        )
    )


def parse_tnc_replacement(trip_raw, tnc_replacement_cols):
    # TODO tnc_replace covert to pl.Enum with tnc_replacement_dict.keys()
    return (
        trip_raw.with_columns(
            pl.col(tnc_replacement_cols).replace({995: None}),
        )
        .with_columns(
            tnc_replace_multi=(pl.sum_horizontal(tnc_replacement_cols) > 1),
        )
        .with_columns(pl.col(tnc_replacement_cols).cast(bool))
        .with_columns(
            tnc_replace=pl.when(pl.col("tnc_replace_multi"))
            .then(pl.lit("multi"))  # multiple TNC replacement selected
            .when(pl.col("tnc_replace_1"))
            .then(pl.lit("taxi"))
            .when(pl.col("tnc_replace_2"))
            .then(pl.lit("drive"))  # drive my own car
            .when(pl.col("tnc_replace_3"))
            .then(pl.lit("shared_ride"))  # ride with family/friend/coworker
            .when(pl.col("tnc_replace_4"))
            .then(pl.lit("BART"))
            .when(pl.col("tnc_replace_5"))
            .then(pl.lit("transit_non_BART"))  # other transit (not BART)
            .when(pl.col("tnc_replace_6"))
            .then(pl.lit("walkbike"))  # walk/bike
            .when(pl.col("tnc_replace_7"))
            .then(pl.lit("other_destination"))  # travel to different place instead
            .when(pl.col("tnc_replace_8"))
            .then(pl.lit("no_trip"))  # would NOT make the trip at all
            .when(pl.col("tnc_replace_9"))
            .then(pl.lit("other"))
        )
    )


def load_trip_raw(
    trip_raw_filepath=trip_raw_filepath,
    num_travelers=False,
    tnc=False,
    tnc_replace=False,
    tnc_wait_time=False,
    taxi_cost=False,
    transit_access_egress=False,
):
    """load raw trip CSV to get raw mode_type and TNC info"""
    columns = ["hh_id", "person_num", "trip_num"]
    tnc_replacement_cols = [f"tnc_replace_{i}" for i in range(1, 10)]
    if num_travelers:
        columns += ["num_travelers"]
    if tnc:
        columns += ["tnc_type", "mode_type"]
    if tnc_replace:
        columns += tnc_replacement_cols
    if tnc_wait_time:
        columns += ["tnc_wait_time"]
    if taxi_cost:
        columns += ["taxi_cost"]
    if transit_access_egress:
        columns += ["transit_access", "transit_egress"]
    trip = pl.read_csv(
        trip_raw_filepath,
        columns=columns,
        dtypes={"tnc_wait_time": float, "taxi_cost": float},
    ).rename({"hh_id": "hhno", "person_num": "pno", "trip_num": "tsvid"})
    if num_travelers:
        trip = trip.with_columns(
            num_travelers_cat=pl.when(pl.col("num_travelers") > 5)
            .then(pl.lit(5))
            .otherwise(pl.col("num_travelers"))
        )
    if tnc_replace:
        trip = parse_tnc_replacement(trip, tnc_replacement_cols)
    return trip


def load_trip_assign_day(trip_assign_day_filepath, depart_hour=False):
    trip = (
        pl.read_csv(trip_assign_day_filepath)
        .filter(
            (pl.col("trexpfac") > 0)  # has to do with weights
            & (pl.col("mode") > 0)  # mode is not other (0)
            & (pl.col("otaz") > 0)  # otaz must exist (i.e. not -1)
            & (pl.col("dtaz") > 0)  # dtaz must exist (i.e. not -1)
        )
        .with_columns(
            count=pl.lit(1)  # NOTE unsure if needed, copied over from 2019 code
        )
    )
    if depart_hour:
        trip = trip.with_columns(
            # NOTE deptm is NOT using standard Daysim definitions, see 02/a-reformat.py
            # NOTE depart_hour was available in the raw data;
            #      but we removed that column in 02/a-reformat.py
            depart_hour=(pl.col("deptm") // 100)
        )
    return trip


def trip_join_hh_person(trip, hh, person):
    return trip.join(hh, on="hhno", how="left").join(
        person, on=["hhno", "pno"], how="left"
    )


def load_tnc_trips(
    tour_extract_dir,
    tnc_replace=False,
    tnc_wait_time=False,
    taxi_cost=False,
    depart_hour=False,
):
    trip = filter_tnc_only(
        load_trip_assign_day(
            tour_extract_dir / "trip-assign_day.csv", depart_hour=depart_hour
        ),
        load_trip_raw(
            tnc=True,
            tnc_replace=tnc_replace,
            tnc_wait_time=tnc_wait_time,
            taxi_cost=taxi_cost,
        ),
    ).to_pandas()
    trip = link_dt(trip)
    return trip


def filter_tnc_only(trip_assign_day, trip_raw):
    """filter for only TNC trips

    Parameters
    ----------
    trip : pl.DataFrame
        trip df with the mode_type column from trip (raw)
    """
    # can't just use `mode` from 2a trip-reformat, because
    # that step combined mode_types 5 (taxi) and 6 (TNC) into mode 9,
    # but we want to exclude taxi trips here
    return trip_assign_day.join(
        trip_raw, on=["hhno", "pno", "tsvid"], how="left"
    ).filter(pl.col("mode_type") == 6)


df_to_excel_col_dict = {  # the output is sorted by the labels list
    "dpurp": {  # from 2a trip-reformat
        "desc": "DPurp",
        "col": "dpurp",
        "vals": range(0, 8),
        "labels": [
            "0_Home",
            "1_Work",
            "2_School",
            "3_Escort",
            "4_PersBus",
            "5_Shop",
            "6_Meal",
            "7_SocRec",
        ],
    },
    # depart_hour = pl.col("deptm") // 100  # with deptm from trip-reformat
    # NOTE deptm is NOT using standard Daysim definitions, see 02/a-reformat.py
    # NOTE depart_hour is in trip (raw); but the col was removed in 02/a-reformat.py
    "depart_hour": {
        "desc": "TOD (depart_hour)",
        "col": "depart_hour",
        "vals": range(0, 24),
        "labels": [f"{i:02}h" for i in range(0, 24)],
    },
    "day": {  # from 3b trip-assign_day
        "desc": "day (DOW)",
        "col": "day",
        "vals": range(1, 8),
        "labels": ["1_Mon", "2_Tue", "3_Wed", "4_Thu", "5_Fri", "6_Sat", "7_Sun"],
    },
    "raceeth": {  # from load_person_demographics()
        "desc": "RaceEth",
        "col": "raceeth",
        "vals": raceeth_dict.keys(),
        "labels": raceeth_dict.values(),
    },
    "hh_income_broad_cat": {  # from load_hh_raw()
        "desc": "HHIncome",
        "col": "hh_income_broad_cat",  # recoded broad categories
        "vals": list(range(1, 7)) + [995, 999],
        "labels": [
            "000-25k",
            "025-50k",
            "050-75k",
            "075-100k",
            "100-200k",
            "200k+",
            "missing response",
            "prefer not to answer",
        ],
    },
    "home_county": {
        "desc": "home_county",
        "col": "home_county",
        "vals": [1, 13, 41, 55, 75, 81, 85, 95, 97],
        "labels": [
            "4_Alameda",
            "5_CCosta",
            "9_Marin",
            "7_Napa",
            "1_SF",
            "2_SanMateo",
            "3_SC",
            "6_Solano",
            "8_Sonoma",
        ],
    },
    "num_travelers_cat": {  # from load_trip_raw()
        "desc": "Number of people in travel party",
        "col": "num_travelers_cat",
        "vals": range(1, 6),
        "labels": ["1", "2", "3", "4", "5+"],
    },
    "pagey": {  # from 2a person-reformat
        "desc": "Age",
        "col": "pagey",
        "vals": [3, 10, 16, 21, 30, 40, 50, 60, 70, 80, 90],
        "labels": [
            "00-5",  # to have it sort correctly
            "05-15",  # to have it sort correctly
            "16-17",
            "18-24",
            "25-34",
            "35-44",
            "45-54",
            "55-64",
            "65-74",
            "75-84",
            "85+",
        ],
    },
    "pgend": {  # from 2a person-reformat
        "desc": "Gend",
        "col": "pgend",
        "vals": [1, 2, 3, 9],
        "labels": ["1_M", "2_F", "3_Other/NonBinary", "9_NA"],
    },
    "tnc_type": {  # from trip (raw)
        "desc": "tnc_type",
        "col": "tnc_type",
        "vals": [1, 2, 3, 995, 998],
        "labels": [
            "1-pooled",
            "2-regular",
            "3-premium",
            "995-missing_response",
            "998-dont_know",
        ],
    },
    "tnc_replace": {  # from load_trip_raw(tnc_replace=True)
        "desc": "tnc_replace",
        "col": "tnc_replace",
        "vals": tnc_replacement_dict.keys(),
        "labels": tnc_replacement_dict.values(),
    },
    # tnc_decide no longer available in 2022 batch
    # "when": {
    #     "desc": "TNC_When",
    #     "col": "tnc_decide",
    #     "vals": list(range(1, 6)) + [995],
    #     "labels": [
    #         "1_RightBef",
    #         "2_Hour",
    #         "3_SameDay",
    #         "4_PrevDay",
    #         "5_2+Days",
    #         "6_Miss",
    #     ],
    # },
    # tnc_schedule no longer available in 2022 batch
    # "sched": {
    #     "desc": "TNC_Sched",
    #     "col": "tnc_schedule",
    #     "vals": list(range(2)) + [995],
    #     "labels": ["1_No", "2_Yes", "3_Miss"],
    # },
}


def county_map_trips(county_df, df):
    county_df.columns = ["dtaz", "dcounty"]
    df = df.merge(county_df, on="dtaz", how="left")
    county_df.columns = ["otaz", "ocounty"]
    df = df.merge(county_df, on="otaz", how="left")
    return df


def trip_dist_map(df):
    df = county_map_trips(county_df, df)
    sup_dist.columns = ["otaz", "osupdist"]
    df = df.merge(sup_dist, how="left", on="otaz")

    df["odist"] = 0
    df.loc[df["ocounty"] == 1, "odist"] = df.loc[df["ocounty"] == 1, "osupdist"]
    for i in range(2, 10):
        df.loc[df["ocounty"].isin([i]), "odist"] = i + 10
    df["odist"] = df["odist"].astype(int)
    return df


def hh_dist_map(df):
    sup_dist.columns = ["hhtaz", "hsupdist"]
    df = df.merge(sup_dist, how="left", on="hhtaz")
    county_df.columns = ["hhtaz", "hcounty"]
    df = df.merge(county_df, how="left", on="hhtaz")

    df["hdist"] = 0
    df.loc[df["hcounty"] == 1, "hdist"] = df.loc[df["hcounty"] == 1, "hsupdist"]
    for i in range(2, 10):
        df.loc[df["hcounty"].isin([i]), "hdist"] = i + 10
    df["hdist"] = df["hdist"].astype(int)
    return df


def link_dt(df):
    """link drive-transit trips"""
    dtrn_df = df.loc[df["dpurp"] == 10,]
    dtrn_df.loc[:, "tseg"] += 1
    dtrn_df = dtrn_df[["hhno", "pno", "day", "tour", "half", "tseg", "otaz", "opurp"]]
    dtrn_df = dtrn_df.rename(columns={"otaz": "otaz_drive", "opurp": "opurp_drive"})
    df = df.loc[df["dpurp"] != 10,]
    df = df.merge(
        dtrn_df, on=["hhno", "pno", "day", "tour", "half", "tseg"], how="left"
    )
    df.loc[df["opurp"] == 10, "otaz"] = df.loc[df["opurp"] == 10, "otaz_drive"]
    df.loc[df["opurp"] == 10, "mode"] = 7
    df.loc[df["opurp"] == 10, "opurp"] = df.loc[df["opurp"] == 10, "opurp_drive"]
    return df


def agg_tnc(df, colname, cat_levels, cat_labels, agg_col):
    cat_levels = list(cat_levels) + ["All"]
    cat_labels = list(cat_labels) + ["All"]
    base_df = pd.DataFrame({"cat_labels": cat_labels, colname: cat_levels})
    df = df[[colname, "count", "trexpfac", agg_col]]
    df["wtd_trips"] = df["count"] * df["trexpfac"]
    df["wtd_metric"] = df[agg_col] * df["trexpfac"]
    df = pd.DataFrame(
        pd.pivot_table(df, index=colname, margins=True, aggfunc="sum")
    ).reset_index()
    df = base_df.merge(df, how="left")
    df = df.fillna(0)
    df["sample_avg"] = df[agg_col] / df["count"]
    df["wtd_avg"] = df["wtd_metric"] / df["wtd_trips"]
    df = df.drop([colname], axis=1)
    df = df.rename(columns={"cat_labels": colname})
    df = df[[colname, "sample_avg", "wtd_avg"]].set_index(colname)
    return df


def getShares(df):
    df.iloc[:, 1:] = df.iloc[:, 1:].apply(lambda x: x / x[:-1].sum(), axis=0) * 100
    df.iloc[:, 1:] = df.iloc[:, 1:].round(1)
    return df


def getSharesIdx(df):
    df = df.apply(lambda x: x / x[:-1].sum(), axis=0) * 100
    df = df.round(1)
    return df


def getRowSharesIdx(df):
    df = df.apply(lambda x: x / x[-1], axis=1) * 100
    df = df.round(1)
    return df


def getSharesIdxCI95(df):
    # TODO just use statsmodels or scipy.stats to calculate CI
    df.iloc[:-1, :] = df.iloc[:-1, :].apply(
        lambda x: (x / x.sum()) * (1 - x / x.sum()), axis=0
    )
    df = df.apply(lambda x: pow(x / x.iloc[-1], 0.5), axis=0)
    df = df.iloc[:-1, :]
    df = (df * 100 * 1.96).round(1)
    return df


def getRowSharesIdxCI95(df):
    df.iloc[:, :-1] = df.iloc[:, :-1].apply(
        lambda x: (x / x.sum()) * (1 - x / x.sum()), axis=1
    )
    df = df.apply(lambda x: pow(x / x[-1], 0.5), axis=1)
    df = df.iloc[:, :-1]
    df = (df * 100 * 1.96).round(1)
    return df


def format_df(df, cols):
    for col in cols:
        df.loc[:, col] = df.apply(lambda x: "{:,}".format(x[col]), axis=1)
    return df


def plotStackedBar(df, cols):
    df = df.transpose()
    df.columns = df.iloc[0,]
    df = df.iloc[1:, :-1]
    df.index.rename("type", inplace=True)
    df = df.loc[cols,]
    return df


def write_to_excel(writer, tab, name, title, row=0):
    t_df = pd.DataFrame({"col1": [title]})
    t_df.to_excel(writer, sheet_name=name, startrow=row, header=False, index=False)
    row += 1

    tab.to_excel(writer, sheet_name=name, startrow=row)
    row += len(tab) + 3
    return row


def prep_data_2d(df, xcol, xvals, xlabels, ycol, yvals, ylabels, valcol):
    base_df = pd.DataFrame([[i, j] for i in xvals for j in yvals])
    base_df.columns = [xcol, ycol]

    df = df.loc[df[valcol] > 0,]
    df = df[[xcol, ycol, valcol]].groupby([xcol, ycol]).sum().reset_index()
    df = base_df.merge(df, how="left")
    df = df.fillna(0)
    #     df[valcol] = df[valcol].astype(int)

    label_df = pd.DataFrame({ycol: yvals, "ylab": ylabels})
    df = df.merge(label_df, how="left")
    label_df = pd.DataFrame({xcol: xvals, "xlab": xlabels})
    df = df.merge(label_df, how="left")
    df = df.drop([xcol, ycol], axis=1)
    df = df.rename(columns={"xlab": xcol, "ylab": ycol})
    df = df.pivot_table(
        index=xcol, columns=ycol, values=valcol, aggfunc="sum", margins=True
    )
    # df_fmt = format_df(df.copy(), df.columns)
    return df  # , df_fmt


def prep_data_1d(df, col_label, colname, wtcol, cat_levels, cat_labels):
    base_df = pd.DataFrame({"cat_labels": cat_labels, colname: cat_levels})

    df = df[[colname, "count", wtcol]].groupby([colname]).sum().reset_index()
    df = df.loc[df[colname].isin(cat_levels),]
    df[colname] = df[colname]

    df = base_df.merge(df, how="left")
    df = df.fillna(0)
    #     df[['count',wtcol]] = df[['count',wtcol]].astype(int)

    col_list = [col_label + "_samp", col_label + "_wtd"]

    df = df.drop([colname], axis=1)
    df = df.rename(
        columns={"cat_labels": colname, "count": col_list[0], wtcol: col_list[1]}
    )
    df = pd.pivot_table(df, index=colname, margins=True, aggfunc="sum")
    #     df.loc[:,col_list] = df.loc[:,col_list].astype(int)
    return df


def agg_1d(df, colname, cat_levels, cat_labels, wtcol, agg_col, agg_name="metric_avg"):
    cat_levels = list(cat_levels) + ["All"]
    cat_labels = cat_labels + ["All"]
    base_df = pd.DataFrame({"cat_labels": cat_labels, colname: cat_levels})

    df = df[[colname, wtcol, agg_col]]
    df[agg_col] = df[agg_col] * df[wtcol]
    df = pd.DataFrame(
        pd.pivot_table(df, index=colname, margins=True, aggfunc="sum")
    ).reset_index()
    df = base_df.merge(df, how="left")
    df = df.fillna(0)
    df[agg_name] = df[agg_col] / df[wtcol]

    df = df.drop([colname], axis=1)
    df = df.rename(columns={"cat_labels": colname})
    df = df[[colname, agg_name]].set_index(colname)
    return df


def agg_2d(df, xcol, xvals, xlabels, ycol, yvals, ylabels, valcol, aggcol):
    df[aggcol] *= df[valcol]
    df_1, dummy = prep_data_2d(
        df.copy(), xcol, xvals, xlabels, ycol, yvals, ylabels, aggcol
    )
    df_2, dummy = prep_data_2d(
        df.copy(), xcol, xvals, xlabels, ycol, yvals, ylabels, valcol
    )
    return df_1 / df_2
