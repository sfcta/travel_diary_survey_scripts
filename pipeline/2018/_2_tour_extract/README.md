The scripts here read in the survey files from the previous step (which reformats and links unlinked trips) and build tours.

1. First, `tour_extract_week.py` is run with `tour_extract_week.ctl` as an argument. The control files contains paths to input and output files.

2. Next, `assign_day.py` is run to calculate and write out weights by person, personday, tour, and trip in addition to producing the person-day file required by DaySim. This is run twice to produce separate sets of files for weekday (Mon-Thu) and all week. The parameters for both cases are in the script itself.
