The scripts here reformat the survey files to attributes used in DaySim - https://github.com/RSGInc/DaySim

For DaySim users guide and other documentation see the wiki - https://github.com/RSGInc/DaySim/wiki

1. First `reformat_survey.py` needs to be run with `reformat_survey.ctl` as an argument. The control files contain paths for input and output files. This requires that the raw survey files are already geocoded to TAZs.

2. `link_trips_week.py` is run next to link any raw reformatted unlinked trips present in the output from the previous step.
