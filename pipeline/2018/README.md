# survey_scripts_tnc2019

This repo contains scripts for processing the MTC-SFCTA TNC Survey 2018-19.

## pipeline
- `00-preprocess.py`
- `01-taz_spatial_join.py`
- `_1_survey_reformat/reformat_survey.py _1_survey_reformat/reformat_survey.ctl`
- `_1_survey_reformat/link_trips_week.py`
- `_2_tour_extract/tour_extract_week.py _2_tour_extract/tour_extract_week.ctl`
- `_2_tour_extract/assign_day.py`
- `_3_merge_skims.py _3_merge_skims.ctl`
- `_3b_adj_weights\_run_adjust_weights.bat`
  - the `master` branch includes kids too, if you want to exclude kids, use the
    `2018-adjust_weights-no_kids` branch (TODO incorporate/implement this as an
    option within the R code rather than as a branch)
- optional: `03c-impute_missing_work_loc.py 03c-config.toml`
  - missing work location imputation was *not* run on the final set of data used
    for comparing 2018 and 2022 vintages of the data (as of March 2025)
### summaries
- `99-r_summaries/updateRsums.bat`

## skim directories for merge_skims
The code needs `HWYALLAM.h5` and `OPTERM.h5` from the skim directory (set in
`merge_skims.ctl`). Some good CHAMP runs / skim directories include:
- csf_2015: `X:\Projects\ConnectSF\2015_TNC_v7_est`
- dtx_2019: `X:\Projects\DTX\CaltrainValidation\s20_2019_Base_noTP`
- champ7ce_Run13_2023: `X:\Projects\CHAMP7CE\Run13_2023`
