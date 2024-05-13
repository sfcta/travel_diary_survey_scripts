from pathlib import Path

import pandas as pd

survey_processed_dir = Path(
    "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2022/Processed_20240329"
)
taz_spatial_join_dir = survey_processed_dir / "01-taz_spatial_join"
reformat_dir = survey_processed_dir / "02-reformat"
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


def prep_modetype_data(df, label, sf_flag=2):
    df = link_dt(df)
    df = df[(df["trexpfac"] > 0) & (df["mode"] > 0)]
    df = df[(df["otaz"] > 0) & (df["dtaz"] > 0)]
    if sf_flag == 0:
        df = df[(df["otaz"] < 1000) | (df["dtaz"] < 1000)]  # to/from/within SF
    elif sf_flag == 1:
        df = df[(df["otaz"] < 1000) & (df["dtaz"] < 1000)]  # only within SF

    df.loc[:, "count"] = 1
    df = df[["mode", "count", "trexpfac"]].groupby(["mode"]).sum().reset_index()
    df = mode_df.merge(df, how="left")
    df = df.fillna(0)
    df = df.sort_values(["modelab"])
    df = df.drop(["mode"], axis=1)

    label_list = [label + "_samp", label + "_wtd"]

    df = df.rename(
        columns={"modelab": "mode", "count": label_list[0], "trexpfac": label_list[1]}
    )
    df = pd.pivot_table(df, index="mode", margins=True, aggfunc="sum").reset_index()
    df.loc[:, label_list] = df.loc[:, label_list].astype(int)
    return df, label_list


def prep_dem_data(df, col_label, colname, wtcol, cat_levels, cat_labels):
    df = df[[colname, "count", wtcol]].groupby([colname]).sum().reset_index()
    df = df.loc[df[colname].isin(cat_levels),]
    df[colname] = df[colname].astype(int)

    label_df = pd.DataFrame({"cat_labels": cat_labels, colname: cat_levels})
    df = label_df.merge(df, how="left")
    df = df.fillna(0)
    df[["count", wtcol]] = df[["count", wtcol]].astype(int)

    col_list = [col_label + "_samp", col_label + "_wtd"]

    df = df.drop([colname], axis=1)
    df = df.rename(
        columns={"cat_labels": colname, "count": col_list[0], wtcol: col_list[1]}
    )
    df = pd.pivot_table(df, index=colname, margins=True, aggfunc="sum").reset_index()
    df.loc[:, col_list] = df.loc[:, col_list].astype(int)
    # df_fmt = format_df(df.copy(), col_list)
    return df, df_fmt


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
    df[colname] = df[colname].astype(int)

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
