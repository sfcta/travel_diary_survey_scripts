
surveydir <- "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2022/Processed_20241127/reformat_2019_rmoveonly/04-merge_skims"
surveyhhfile <- "survey2023_hrecx.dat"
surveyperfile <- "survey2023_precx.dat"
surveypdayfile <- "survey2023_pdayx.dat"
surveytourfile <- "survey2023_tourx.dat"
surveytripfile <- "survey2023_tripx.dat"

CONTROL_SOURCE_DAYSIM = TRUE
CONTROL_SOURCE_SFSAMP = FALSE

synthdir <- "Q:/Model Development/CHAMP7CE/Scenarios/Run0_2023/Inputs/landuse_daysim"
synth_hhfile <- "_hrecx.dat"
synth_perfile <- "_precx.dat"

NUM_PTYPES <- 6

outdir <- "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2022/Processed_20241127/reformat_2019_rmoveonly/04-merge_skims/adj_weights_nokids"
outhhfile <- "survey2023_hrecx_rewt_base2023.dat"
outperfile <- "survey2023_precx_rewt_base2023.dat"
outpdayfile <- "survey2023_pdayx_rewt_base2023.dat"
outtourfile <- "survey2023_tourx_rewt_base2023.dat"
outtripfile <- "survey2023_tripx_rewt_base2023.dat"