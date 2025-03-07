set THISDIR="Q:\Data\Surveys\HouseholdSurveys\MTC-SFCTA2022\Processed_20241127\reformat_2019_rmoveonly\survey_scripts_tnc2019"
set RSUMMDIR="Q:\Data\Surveys\HouseholdSurveys\MTC-SFCTA2022\Processed_20241127\R_Summaries_update_sur-v-sur"

pushd %CD%
cd /d %RSUMMDIR%
@REM R installed via scoop on CHAVEZ
@REM "C:\Program Files\R\R-4.0.5\bin\x64\R.exe" 
@REM "C:\Program Files\R\R-4.0.5\bin\R.exe"
"C:\Program Files\R\R-4.0.5\bin\R.exe" CMD BATCH --no-save "--args %THISDIR%\updateRsums.R" main.R "%THISDIR%\RSummaries_survey.log"
::"C:\Users\joe\scoop\apps\r\current\bin\R.exe" CMD BATCH --no-save "--args %THISDIR%\daysim_output_config-output-surveys-2023ref-vs-2019-20250110a.R" main.R "%THISDIR%\RSummaries_survey_log.txt"
popd

