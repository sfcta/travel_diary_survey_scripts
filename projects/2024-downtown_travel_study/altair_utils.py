import altair as alt

color_transit_teal = "#006c69"
color_value_transit_teal = alt.value(color_transit_teal)

color_covid_pre_post_both = ["#ff8d6b", "#63acc9", "#6d6d6d"]
color_worse_better_neutral = color_covid_pre_post_both

color_line_blue_green = ["#73abbe", "#cccc3d"]
color_fill_blue_green = ["#5eb3e4", "#d3d655"]

color_line_yellow_purple = ["#ffb81d", "#cd7f9e"]
color_fill_yellow_purple = ["#fac32c", "#cd7f9e"]

color_line_muni_bart_caltrain = ["#ff8d6b", "#8cb7c9", "#808080"]

color_weekday_weekend = ["#5eb3e4", "#a0d0cb"]

color_sf_bayarea = ["#358d89", "#bbbbbb"]
color_sfcore_restofsf_restofbayarea = [color_transit_teal, "#6aaeaa", "#d2d2d2"]
color_sfubercore_restofsfcore_restofsf = [
    "#003b49"
] + color_sfcore_restofsf_restofbayarea[:-1]

color_collision_ped_bike = ["#cd7f9e", "#fea586"]

color_line_modes = {
    "DA": "#ff8d6d",  # also for all drive (DA + Carpool)
    "Carpool": "#ffc6b2",  # i.e. HOV2 + HOV3+
    "HOV2": "#f1a98c",
    "HOV3+": "#f5bfa7",
    "Taxi/TNC": "#f6cf3f",
    "Bike": "#b9c279",
    "Walk": "#8cb8ca",
    "Transit": "#cd7f9e",
    "Other": "#d2d2d2",
    # for e.g. Walk+Bike+Other, pick the highest mode-share mode color
    # (i.e. Walk in this case)
    "Work from Home": "#6d6d6d",  # dashed line  # if fill: hashed
}

line_strokeWidth = 3
line_strokeDash_dash = [line_strokeWidth * 4 / 3, line_strokeWidth * 8 / 3]
line_strokeDash = [[1, 0], line_strokeDash_dash]


def theme_sfcta():
    return {
        "config": {
            "font": "Libre Franklin",
            "axis": {
                "labelAngle": 0,
                "labelFlush": False,
                "labelFontSize": 12,
                "titleFontSize": 12,
                # if we set "ticks": False, the spacing gets messed up
                "tickWidth": 0,  # so we set the tickWidth to 0 instead
                # "tickColor": "#cccccc",
                "gridWidth": 0.25,
                "gridColor": "#cccccc",
            },
            "axisX": {
                "domainWidth": 0.5,
                "domainColor": "#3e3e3e",
                "titleAlign": "right",
                "titleAnchor": "end",
            },
            "axisY": {
                "domain": False,
                # turn off for now since cannot figure out spacing:
                # "labelAlign": "left",
                "titleAlign": "left",
                "titleAnchor": "end",
                "titleAngle": 0,
                "titlePadding": 0,
                "titleY": -8,
            },
            "line": {"strokeWidth": line_strokeWidth, "strokeCap": "round"},
            "legend": {"title": None},
            "view": {"stroke": None},
        }
    }


alt.themes.register("sfcta", theme_sfcta)
alt.themes.enable("sfcta")


# things to set for final html export:
# - set axisY "ticksize" to set tick length so as to not have gridlines overlap labels
# - axis title text a) allcaps, b) letter spacing (typographic "tracking")
