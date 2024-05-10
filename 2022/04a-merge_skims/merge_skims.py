import datetime
import sys
from pathlib import Path

import pandas as pd
import polars as pl
from HwySkimUtil import SkimUtil2

delim = " "

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Please provide a control file which contains all the required input parameters as an argument!"
        )
    else:
        print("Merge skims program started: " + str(datetime.datetime.now()))
        # Initiate log file
        logfilename = "skim_merge.log"
        logfile = open(logfilename, "w")
        logfile.write(
            "Merge skims program started: " + str(datetime.datetime.now()) + "\n"
        )

        inputctlfile = sys.argv[1]
        ctlfile = open(inputctlfile)
        for ctlfileline in ctlfile:
            logfile.write(ctlfileline)
            if len(str.split(ctlfileline)) > 1:
                param = (str.split(ctlfileline)[0]).upper()
                value = str.split(ctlfileline)[1]
                if param == "INDIR":
                    inputdir = Path(value)
                elif param == "INHHFILE":
                    inhhfilename = value
                elif param == "INPERFILE":
                    inperfilename = value
                elif param == "INPDAYFILE":
                    inpdayfilename = value
                elif param == "INTOURFILE":
                    intourfilename = value
                elif param == "INTRIPFILE":
                    intripfilename = value
                elif param == "SKIMDIR":
                    skimdir = value

                elif param == "OUTDIR":
                    outputdir = Path(value)
                elif param == "OUTHHFILE":
                    outhhfilename = value
                elif param == "OUTPERFILE":
                    outperfilename = value
                elif param == "OUTPDAYFILE":
                    outpdayfilename = value
                elif param == "OUTTOURFILE":
                    outtourfilename = value
                elif param == "OUTTRIPFILE":
                    outtripfilename = value

        inhhfilename = inputdir / inhhfilename
        inperfilename = inputdir / inperfilename
        intourfilename = inputdir / intourfilename
        intripfilename = inputdir / intripfilename
        inpdayfilename = inputdir / inpdayfilename

        outputdir.mkdir(exist_ok=True)
        outhhfilename = outputdir / outhhfilename
        outperfilename = outputdir / outperfilename
        outtourfilename = outputdir / outtourfilename
        outtripfilename = outputdir / outtripfilename
        outpdayfilename = outputdir / outpdayfilename

        tour = pd.read_csv(intourfilename)
        print("Read tour file: " + str(datetime.datetime.now()))
        logfile.write("Read tour file: " + str(datetime.datetime.now()) + "\n")
        trip = pd.read_csv(intripfilename)
        print("Read trip file: " + str(datetime.datetime.now()))
        logfile.write("Read trip file: " + str(datetime.datetime.now()) + "\n")

        WKDAY_DOW = range(1, 5)
        NONWKDAY_DOW = [5, 6, 7]

        pday = pd.read_csv(inpdayfilename)
        # output pday file  # TODO do NOT output pday, given it is entirely unchanged
        pday.to_csv(outpdayfilename, index=False, sep=delim)
        # some old code
        #         out_cols = pday.columns
        #         temp_df = pday[pday['day'].isin(WKDAY_DOW)]
        #         temp_df = pday.temp_df(['hhno','pno'])['day'].agg('count').reset_index().rename(columns={'day':'wkday_count'})
        #         pday = pday.merge(temp_df, on=['hhno','pno'], how='left')
        #         pday['pdexpfac'] = pday['pdexpfac']/pday['wkday_count']
        #         pday.loc[pday['day'].isin(NONWKDAY_DOW), 'pdexpfac'] = 0
        #         pday[out_cols].to_csv(outpdayfilename, index=False, sep=delim)

        # Initialize skim utility
        skimutil = SkimUtil2(skimdir)

        # Retain SFCTA persons and merge work and school skims
        logfile.write("\n")
        logfile.write(
            "Merge skims for persons started: " + str(datetime.datetime.now()) + "\n"
        )
        print("Merge skims for persons started: " + str(datetime.datetime.now()))

        # hh is just to get hhtaz to join to person
        hh = pl.read_csv(inhhfilename)
        # output hh file  # TODO do NOT output hh, given it is entirely unchanged
        hh.write_csv(outhhfilename, separator=delim)

        person = (
            pl.read_csv(inperfilename)
            .join(hh.select("hhno", "hhtaz"), on="hhno", how="left")
            .to_pandas()
        )
        person["pwautime"] = -1.0
        person["pwaudist"] = -1.0
        person["psautime"] = -1.0
        person["psaudist"] = -1.0
        for i in range(len(person)):
            otaz = int(person["hhtaz"][i])
            dtaz = int(person["pwtaz"][i])
            if otaz > 0 and dtaz > 0:
                skims = skimutil.getDASkims(otaz, dtaz)
                person.loc[i, "pwautime"] = skims[0]
                person.loc[i, "pwaudist"] = skims[1]
            dtaz = int(person["pstaz"][i])
            if otaz > 0 and dtaz > 0:
                skims = skimutil.getDASkims(otaz, dtaz)
                person.loc[i, "psautime"] = skims[0]
                person.loc[i, "psaudist"] = skims[1]

        person.to_csv(outperfilename, sep=delim, index=False)
        logfile.write(
            "Merge skims for persons finished: " + str(datetime.datetime.now()) + "\n"
        )
        print("Merge skims for persons finished: " + str(datetime.datetime.now()))

        # Merge skims into tour
        logfile.write("\n")
        logfile.write(
            "Merge skims for tour started: " + str(datetime.datetime.now()) + "\n"
        )
        print("Merge skims for tour started: " + str(datetime.datetime.now()))

        tour["tautotime"] = -1.0
        tour["tautocost"] = -1.0
        tour["tautodist"] = -1.0
        for i in range(len(tour)):
            otaz = int(tour["totaz"][i])
            dtaz = int(tour["tdtaz"][i])
            if otaz > 0 and dtaz > 0:
                skims = skimutil.getDASkims(otaz, dtaz)
                tour.loc[i, "tautotime"] = skims[0]
                tour.loc[i, "tautocost"] = skims[2]
                tour.loc[i, "tautodist"] = skims[1]

        out_cols = tour.columns
        tour = tour.merge(
            pday[["hhno", "pno", "day", "pdexpfac"]],
            on=["hhno", "pno", "day"],
            how="left",
        )
        tour["toexpfac"] = tour["pdexpfac"]
        tour.to_csv(outtourfilename, sep=delim, index=False)
        logfile.write(
            "Merge skims for tour finished: " + str(datetime.datetime.now()) + "\n"
        )
        print("Merge skims for tour finished: " + str(datetime.datetime.now()))

        # Merge skims into trip
        logfile.write("\n")
        logfile.write(
            "Merge skims for trip started: " + str(datetime.datetime.now()) + "\n"
        )
        print("Merge skims for trip started: " + str(datetime.datetime.now()))

        trip["travtime"] = -1.0
        trip["travcost"] = -1.0
        trip["travdist"] = -1.0
        for i in range(len(trip)):
            otaz = int(trip["otaz"][i])
            dtaz = int(trip["dtaz"][i])
            if otaz > 0 and dtaz > 0:
                skims = skimutil.getDASkims(otaz, dtaz)
                trip.loc[i, "travtime"] = skims[0]
                trip.loc[i, "travcost"] = skims[2]
                trip.loc[i, "travdist"] = skims[1]

        out_cols = trip.columns
        trip = trip.merge(
            pday[["hhno", "pno", "day", "pdexpfac"]],
            on=["hhno", "pno", "day"],
            how="left",
        )
        trip["trexpfac"] = trip["pdexpfac"]
        trip.to_csv(outtripfilename, sep=delim, index=False)
        logfile.write(
            "Merge skims for trip finished: " + str(datetime.datetime.now()) + "\n"
        )
        print("Merge skims for trip finished: " + str(datetime.datetime.now()))

        logfile.write("\n")
        logfile.write(
            "Merge skims program finished: " + str(datetime.datetime.now()) + "\n"
        )
        logfile.close()
        print("Merge skims program finished: " + str(datetime.datetime.now()))
