
# dshhfile                                  = "%MODELRUNDIR%/daysim/abm_output1/_household_2.dat"
# dsperfile                                 = "%MODELRUNDIR%/daysim/abm_output1/_person_2.dat"
# dspdayfile                                = "%MODELRUNDIR%/daysim/abm_output1/_person_day_2.dat"
# dstourfile                                = "%MODELRUNDIR%/daysim/abm_output1/_tour_2.dat"
# dstripfile                                = "%MODELRUNDIR%/daysim/abm_output1/_trip_2.dat"
# dstriplistfile                            = "%MODELRUNDIR%/daysim/abm_output1/Tdm_trip_list.csv"

# for now, set the 2019 survey results as the "Daysim" output files for input into R instead
# dshhfile                                  = "_old_survey/sfcta_chts_hrecx_rewt_abmxfer.dat"
# dsperfile                                 = "_old_survey/sfcta_chts_precx_rewt_abmxfer.dat"
# dspdayfile                                = "_old_survey/sfcta_chts_pdayx_rewt_abmxfer.dat"
# dstourfile                                = "_old_survey/sfcta_chts_tourx_rewt_abmxfer.dat"
# dstripfile                                = "_old_survey/sfcta_chts_tripx_rewt_abmxfer.dat"
dsdir                                 = "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2018/Processing_20211018/v00/3_merge_skims-250102/adj_weights"
dshhfile                              = file.path(dsdir,"survey2018_hrecx_rewt_base2019.dat")
dsperfile                             = file.path(dsdir,"survey2018_precx_rewt_base2019.dat")
dspdayfile                            = file.path(dsdir,"survey2018_pdayx_rewt_base2019.dat")
dstourfile                            = file.path(dsdir,"survey2018_tourx_rewt_base2019.dat")
dstripfile                            = file.path(dsdir,"survey2018_tripx_rewt_base2019.dat")
dstriplistfile                        = file.path("X:/Projects/CHAMP7CE/Run5_2023/daysim/abm_output1","tdm_trip_list.csv")

# Survey
# surveyhhfile                                  = "_old_survey/sfcta_chts_hrecx_rewt_abmxfer.dat"
# surveyperfile                                 = "_old_survey/sfcta_chts_precx_rewt_abmxfer.dat"
# surveypdayfile                                = "_old_survey/sfcta_chts_pdayx_rewt_abmxfer.dat"
# surveytourfile                                = "_old_survey/sfcta_chts_tourx_rewt_abmxfer.dat"
# surveytripfile                                = "_old_survey/sfcta_chts_tripx_rewt_abmxfer.dat"
surveydir = "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2022/Processed_20241127/reformat_2019_rmoveonly/04-merge_skims/adj_weights"
surveyhhfile                              = file.path(surveydir,"survey2023_hrecx_rewt_base2023.dat")
surveyperfile                             = file.path(surveydir,"survey2023_precx_rewt_base2023.dat")
surveypdayfile                            = file.path(surveydir,"survey2023_pdayx_rewt_base2023.dat")
surveytourfile                            = file.path(surveydir,"survey2023_tourx_rewt_base2023.dat")
surveytripfile                            = file.path(surveydir,"survey2023_tripx_rewt_base2023.dat")

tazcountycorr                             = "./data/taz_districts_sfcta.csv"
mazfile                                   = "./data/maz_stline_buffer.dat"
ixxifile                                  = "./data/sfcta_ixxi.dat"

wrklocmodelfile                           = "./templates/WrkLocation.csv"
schlocmodelfile                           = "./templates/SchLocation.csv"
vehavmodelfile                            = "./templates/VehAvailability.csv"
daypatmodelfile1                          = "./templates/DayPattern_pday.csv"
daypatmodelfile2                          = "./templates/DayPattern_tour.csv"
daypatmodelfile3                          = "./templates/DayPattern_trip.csv"
tourdestmodelfile                         = "./templates/TourDestination.csv"
tourdestwkbmodelfile                      = "./templates/TourDestination_wkbased.csv"
tripdestmodelfile                         = "./templates/TripDestination.csv"
tourmodemodelfile                         = "./templates/TourMode.csv"
tourtodmodelfile                          = "./templates/TourTOD.csv"
tripmodemodelfile                         = "./templates/TripMode.csv"
triptodmodelfile                          = "./templates/TripTOD.csv"

wrklocmodelout                            = "WrkLocation.xlsm"
schlocmodelout                            = "SchLocation.xlsm"
vehavmodelout                             = "VehAvailability.xlsm"
daypatmodelout                            = "DayPattern.xlsm"
tourdestmodelout                          = c("TourDestination_Work.xlsm","TourDestination_School.xlsm","TourDestination_Escort.xlsm","TourDestination_PerBus.xlsm",
                                              "TourDestination_Shop.xlsm","TourDestination_Meal.xlsm","TourDestination_SocRec.xlsm")
tourdestwkbmodelout                       = "TourDestination_WrkBased.xlsm"
tourmodemodelout                          = "TourMode.xlsm"
tourtodmodelout                           = "TourTOD.xlsm"
tripmodemodelout                          = "TripMode.xlsm"
triptodmodelout                           = "TripTOD.xlsm"
mazwrklocout                              = "MazWrkSch.xlsm"

outputsDir                                = "Q:/Data/Surveys/HouseholdSurveys/MTC-SFCTA2022/Processed_20241127/reformat_2019_rmoveonly/99-rsums"  # "X:/Projects/CHAMP7CE/Run5_2023/daysim/r_summaries"
validationDir                             = ""

prepSurvey                                = TRUE
prepDaySim                                = TRUE

runWrkSchLocationChoice                   = TRUE
runVehAvailability                        = TRUE
runDayPattern                             = TRUE
runTourDestination                        = TRUE
runTourMode                               = TRUE
runTourTOD                                = TRUE
runTripMode                               = TRUE
runTripTOD                                = TRUE

excludeChildren_All                       = FALSE
excludeChildren5                          = FALSE
tourAdj                                   = FALSE
tourAdjFile				                  = "./data/peradjfac.csv"
