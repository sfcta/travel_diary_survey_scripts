'''
Created on Jan 15, 2020

@author: bsana
'''
from os.path import join
import sys,datetime
import pandas as pd

OUT_SEP = ' '
COUNTY_FIPS = [1,13,41,55,75,81,85,95,97]

if __name__ == '__main__':
    
    if len(sys.argv)<2:
        print('Please provide a control file which contains all the required input parameters as an argument!')
    else:
        print('Reformat survey program started: {}'.format(datetime.datetime.now()))
        #Initiate log file
        logfilename = 'reformat_survey.log'
        logfile = open(logfilename,'w')
        logfile.write('Reformat survey program started: ' + str(datetime.datetime.now()) + '\n')
        
        inputctlfile = sys.argv[1]
        ctlfile = open(inputctlfile)
        for ctlfileline in ctlfile:
            logfile.write(ctlfileline)
            if len(str.split(ctlfileline))>1:
                param = (str.split(ctlfileline)[0]).upper()
                value = str.split(ctlfileline)[1]
                if param == 'INDIR':
                    inputdir = value
                elif param == 'INHHFILE':
                    inhhfilename = value
                elif param == 'INPERFILE':
                    inperfilename = value
                elif param == 'INTRIPFILE':
                    intripfilename = value
                elif param == 'INDAYFILE':
                    indayfilename = value
                
                elif param == 'OUTDIR':
                    outputdir = value
                elif param == 'OUTHHFILE':
                    outhhfilename = value
                elif param == 'OUTPERFILE':
                    outperfilename = value
                elif param == 'OUTTRIPFILE':
                    outtripfilename = value
        
        inhhfilename = join(inputdir, inhhfilename)
        inperfilename = join(inputdir, inperfilename)
        intripfilename = join(inputdir, intripfilename)
        if indayfilename:
            indayfilename = join(inputdir, indayfilename)
        
        outhhfilename = join(outputdir, outhhfilename)
        outperfilename = join(outputdir, outperfilename)
        outtripfilename = join(outputdir, outtripfilename)
        
        ###### Household file processing
        print('Household file processing started: {}'.format(datetime.datetime.now()))
        logfile.write('\n')                
        logfile.write('Household file processing started: ' + str(datetime.datetime.now()) + '\n')
        
        hh = pd.read_csv(inhhfilename)
        
        hh['hhno'] = hh['hh_id']
        hh['hhsize'] = hh['num_people']
        hh.loc[hh['hhsize']>900, 'hhsize'] =  -1
        
        hh['hhvehs'] = hh['num_vehicles']
        hh.loc[hh['hhvehs']>900, 'hhvehs'] =  -1
        
        INC1_DICT = {999:-1, 1:7500, 2:20000, 3:30000, 4:42500, 5:62500, 6:87500, 7:125000, 8:175000, 9:225000, 10:350000}
        hh['hhincome'] = hh['income_detailed'].map(INC1_DICT)
        INC2_DICT = {999:-1, 1:12500, 2:37500, 3:62500, 4:87500, 5:175000, 6:350000}
        hh['hhinc2'] = hh['income_followup'].map(INC2_DICT)
        hh['hhinc3'] = hh['hhinc_imputed'].map(INC2_DICT)
        hh['hhinc4'] = hh['hhinc_nonrel_imputed'].map(INC2_DICT)
        
        # hhinc4 is the household income after adjusting for missing income of unrelated household members.  
        # use hhinc4 if hhincome is missing, or if hhinc4 is greater than the others.  
        hh.loc[(hh['hhincome']<0) | (hh['hhinc4']>hh['hhincome']), 'hhincome'] = hh.loc[(hh['hhincome']<0) | (hh['hhinc4']>hh['hhincome']), 'hhinc4']
        #hh.loc[(hh['hhincome']<0) & (hh['hhinc2']>0), 'hhincome'] = hh.loc[(hh['hhincome']<0) & (hh['hhinc2']>0), 'hhinc2']
        
        hh['hownrent'] = hh['rent_own']
        hh.loc[hh['hownrent']==997, 'hownrent'] = 3 #Other
        hh.loc[hh['hownrent']==995, 'hownrent'] = 9 # UPDATED DC 1/23/2025
        hh.loc[hh['hownrent']==999, 'hownrent'] = 9 #Prefer not to answer -> Missing
        hh.loc[hh['hownrent']<0, 'hownrent'] = -1
        
        RESTYPE_DICT = {1:1, 2:2, 3:3, 4:3, 5:3, 6:5, 7:4, 997:6}
        hh['hrestype'] = hh['res_type'].map(RESTYPE_DICT)
        hh.loc[pd.isnull(hh['hrestype']), 'hrestype'] = -1
        
        hh['hxcord'] = hh['reported_home_lon']
        hh['hycord'] = hh['reported_home_lat']
        
        int_cols = ['hhparcel','hhtaz','hhincome','hrestype'] 
        hh[int_cols] = hh[int_cols].astype(int)
        
        out_colnames = ['hhno','hhsize','hhvehs','hhincome','hownrent','hrestype','hhparcel','hhtaz','hxcord','hycord','wt_alladult_wkday','wt_alladult_7day']
        
        hh = hh[out_colnames]
        hh = hh.sort_values('hhno')
        hh.to_csv(outhhfilename, sep=OUT_SEP, index=False)
        
        print('Household file processing finished: {}'.format(datetime.datetime.now()))                
        logfile.write('Household file processing finished: ' + str(datetime.datetime.now()) + '\n')
        
        ###### Person file processing
        print('Person file processing started: {}'.format(datetime.datetime.now()))
        logfile.write('\n')                
        logfile.write('Person file processing started: ' + str(datetime.datetime.now()) + '\n')
        
        per = pd.read_csv(inperfilename)
        per['person_id'] = per['person_id'].round()
        
        per['hhno'] = per['hh_id']
        per['pno'] = per['person_num']
        per = per.merge(hh[['hhno','hxcord','hycord','hhparcel','hhtaz']])
        
        AGE_DICT = {1:3, 2:10, 3:16, 4:21, 5:30, 6:40, 7:50, 8:60, 9:70, 10:80}
        per['pagey'] = per['age'].map(AGE_DICT)
        
        GEND_DICT = {1:2, 2:1, 3:3, 4:3, 997:3, 995:9, 999:9}
        per['pgend'] = per['gender'].map(GEND_DICT)
        per.loc[per['pgend']<0, 'pgend'] = -1
        per.loc[pd.isna(per['pgend']), 'pgend'] = -1
        
        per['pptyp'] =  0
        per['pwtyp'] =  0
        per['pstyp'] =  -1
        
        per.loc[(per['pagey']>=0) & (per['pagey']<5), 'pptyp'] = 8
        per.loc[(per['pagey']>=0) & (per['pagey']<16) & (per['pptyp']==0), 'pptyp'] = 7
        per.loc[(per['employment'].isin([1,3,7,8])) & (per['hours_work'].isin([1,2,3,4])) & (per['pptyp']==0), 'pptyp'] = 1 # UPDATED DC 1/23/2025
        per.loc[(per['pagey']>=16) & (per['pagey']<18) & (per['student'].isin([1,2])) & (per['pptyp']==0), 'pptyp'] = 6
        per.loc[(per['pagey']>=16) & (per['pagey']<25) & (per['school_type'].isin([7])) & (per['student'].isin([1,2])) & (per['pptyp']==0), 'pptyp'] = 6
        per.loc[(per['student'].isin([1,2])) & (per['pptyp']==0), 'pptyp'] = 5
        per.loc[(per['employment'].isin([1,2,3,7,8])) & (per['pptyp']==0), 'pptyp'] = 2 # Remaining workers are part-time # UPDATED DC 1/23/2025
        per.loc[(per['pagey']>65) & (per['pptyp']==0), 'pptyp'] = 3
        per.loc[per['pptyp']==0, 'pptyp'] = 4
        
        per.loc[per['pptyp']==1, 'pwtyp'] = 1
        per.loc[per['pptyp']==2, 'pwtyp'] = 2
        # student workers are also part-time workers
        per.loc[(per['pptyp']==5) & (per['employment'].isin([1,2,3])), 'pwtyp'] = 2
        per.loc[(per['pptyp']==6) & (per['employment'].isin([1,2,3])), 'pwtyp'] = 2
        
        per.loc[per['student']==0, 'pstyp'] = 0
        per.loc[per['student']==1, 'pstyp'] = 1
        per.loc[per['student']==2, 'pstyp'] = 2
        
        per['pwxcord'] = per['work_lon']
        per['pwycord'] = per['work_lat']
        
        per['psxcord'] = per['school_lon']
        per['psycord'] = per['school_lat']
        
        per['ppaidprk'] = -1 # UPDATED DC 1/23/2025
        per.loc[per['work_park'].isin([1,2]), 'ppaidprk'] = 0 # UPDATED DC 1/23/2025
        per.loc[per['work_park'].isin([3,4]), 'ppaidprk'] = 1 # UPDATED DC 1/23/2025
        
        per = per.rename(columns={'pwtaz':'pwtaz_tmp', 'pstaz':'pstaz_tmp',
                                  'pwpcl':'pwpcl_tmp', 'pspcl':'pspcl_tmp'})
        per.loc[per['work_county_fips'].isin(COUNTY_FIPS), 'pwtaz'] = per.loc[per['work_county_fips'].isin(COUNTY_FIPS), 'pwtaz_tmp']
        per.loc[per['work_county_fips'].isin(COUNTY_FIPS), 'pwpcl'] = per.loc[per['work_county_fips'].isin(COUNTY_FIPS), 'pwpcl_tmp']
        per.loc[per['school_county_fips'].isin(COUNTY_FIPS), 'pstaz'] = per.loc[per['school_county_fips'].isin(COUNTY_FIPS), 'pstaz_tmp']
        per.loc[per['school_county_fips'].isin(COUNTY_FIPS), 'pspcl'] = per.loc[per['school_county_fips'].isin(COUNTY_FIPS), 'pspcl_tmp']
        
        # UPDATED DC 1/23/2025
        #per['flag'] = 0
        #per.loc[(per['pwtyp']>0) & (per['job_type']==3), 'flag'] = 1 # Work at home ONLY (telework, self-employed)
        #per.loc[per['flag']==1, 'pwxcord'] = per.loc[per['flag']==1, 'hxcord']
        #per.loc[per['flag']==1, 'pwycord'] = per.loc[per['flag']==1, 'hycord']
        #per.loc[per['flag']==1, 'pwpcl'] = per.loc[per['flag']==1, 'hhparcel']
        #per.loc[per['flag']==1, 'pwtaz'] = per.loc[per['flag']==1, 'hhtaz']
        
        per.loc[pd.isnull(per['pwtaz']), 'pwtaz'] = -1
        per.loc[pd.isnull(per['pstaz']), 'pstaz'] = -1
        per.loc[pd.isnull(per['pwpcl']), 'pwpcl'] = -1
        per.loc[pd.isnull(per['pspcl']), 'pspcl'] = -1
        per.loc[pd.isnull(per['pwxcord']), 'pwxcord'] = -1.0
        per.loc[pd.isnull(per['pwycord']), 'pwycord'] = -1.0
        per.loc[pd.isnull(per['psxcord']), 'psxcord'] = -1.0
        per.loc[pd.isnull(per['psycord']), 'psycord'] = -1.0
        
        # there appear to be some person records who are not students but have school loc Ex. hhid 181005890
        # account for that by setting school loc to null/missing
        per.loc[per['pstyp']==0, 'pstaz'] = -1
        per.loc[per['pstyp']==0, 'pspcl'] = -1
        per.loc[per['pstyp']==0, 'psxcord'] = -1.0
        per.loc[per['pstyp']==0, 'psycord'] = -1.0
        
        # there appear to be some person records who are not workers but have work loc Ex. hhid 181007697
        # account for that by setting work loc to null/missing
        per.loc[per['pwtyp']==0, 'pwtaz'] = -1
        per.loc[per['pwtyp']==0, 'pwpcl'] = -1
        per.loc[per['pwtyp']==0, 'pwxcord'] = -1.0
        per.loc[per['pwtyp']==0, 'pwycord'] = -1.0
        
        int_cols = ['pwtaz','pstaz','pwpcl','pspcl','pgend'] 
        per[int_cols] = per[int_cols].astype(int)
        
        out_colnames = ['hhno','pno','pptyp','pagey','pgend','pwtyp','pwpcl','pwtaz','pstyp','pspcl','pstaz','ppaidprk','pwxcord','pwycord','psxcord','psycord']
        out_colnames = out_colnames + ['wt_alladult_wkday','wt_alladult_7day']
        out_colnames = out_colnames + ['mon_complete','tue_complete','wed_complete','thu_complete','fri_complete',
                                       'sat_complete','sun_complete','nwkdaywts_complete','n7daywts_complete']
        per = per[out_colnames]
        per = per.sort_values(['hhno','pno'])
        per.to_csv(outperfilename, sep=OUT_SEP, index=False)
        
        print('Person file processing finished: {}'.format(datetime.datetime.now()))              
        logfile.write('Person file processing finished: ' + str(datetime.datetime.now()) + '\n')
        
        ###### Trip processing
        print('Trip file processing started: {}'.format(datetime.datetime.now()))
        logfile.write('\n')                
        logfile.write('Trip file processing started: ' + str(datetime.datetime.now()) + '\n')
        
        trip = pd.read_csv(intripfilename)
        trip['person_id'] = trip['person_id'].round()
        
        trip['hhno'] = trip['hh_id']
        trip['pno'] = trip['person_num']
        if 'trip_num' in trip.columns:
            trip['tripno'] = trip['trip_num']
        else:
            trip['tripno'] = trip['linked_trip_id']
        
        if 'travel_date_dow' not in trip.columns:
            day = pd.read_csv(indayfilename)
            day = day[['person_id','day_num','travel_date_dow']]
            day['person_id'] = day['person_id'].round()
            trip = trip.merge(day, on=['person_id','day_num']) 
        trip['dow'] = trip['travel_date_dow'] 
        
        # retain only complete day trips
        trip = trip.merge(per[['hhno','pno','mon_complete','tue_complete','wed_complete','thu_complete',
                               'fri_complete','sat_complete','sun_complete']],
                            how='left', on=['hhno','pno'])
        trip['incomplete_day_flag'] = 0
        for i,d in zip(range(1,8), ['mon','tue','wed','thu','fri','sat','sun']):
            trip.loc[(trip['dow']==i) & (trip[d+'_complete']==0), 'incomplete_day_flag'] = 1
        trip = trip[trip['incomplete_day_flag']==0]
        
        PURP_DICT = {
            -1:-1,  #missing -> missing
            1:0,    #home -> home
            2:1,    #work -> work
            3:4,    #work-related -> personal business
            4:2,    #school -> school
            5:3,    #escort -> escort
            6:5,    #shop -> shop
            7:6,    #meal -> meal
            8:7,    #socrec -> socrec
            9:4,    #errand/other -> pers.bus.
            10:10,  #change mode -> change mode
            11:11,  #night non-home -> other
            12:11,  #other\missing -> other
            14:4,  #trip needs to be merged -> other
            }
        trip.loc[pd.isna(trip['o_purpose_category_imputed']), 'o_purpose_category_imputed'] = -1
        trip.loc[pd.isna(trip['d_purpose_category_imputed']), 'd_purpose_category_imputed'] = -1
        trip['opurp'] = trip['o_purpose_category_imputed'].map(PURP_DICT)
        trip['dpurp'] = trip['d_purpose_category_imputed'].map(PURP_DICT)
        
        #0-other 1-walk 2-bike 3-DA 4-hov2 5-hov3 6-walktran 7-drivetran 8-schbus 9-tnc
        trip['mode'] = 0
        trip.loc[(trip['mode_type_imputed']==1) & (trip['mode']==0), 'mode'] = 1
        trip.loc[(trip['mode_type_imputed']==2) & (trip['mode']==0), 'mode'] = 2
        trip.loc[(trip['mode_type_imputed']==11) & (trip['mode']==0), 'mode'] = 2
        trip.loc[(trip['mode_type_imputed']==12) & (trip['mode']==0), 'mode'] = 2
        
        DRIVE_MODES = [3, 10]
        trip.loc[(trip['mode_type_imputed'].isin(DRIVE_MODES)) & (trip['mode']==0), 'mode'] = 3
        trip.loc[(trip['mode']==3) & (trip['num_travelers']==2), 'mode'] = 4
        trip.loc[(trip['mode']==3) & (trip['num_travelers']>2), 'mode'] = 5
        
        trip.loc[(trip['mode_type_imputed']==4) & (trip['mode']==0), 'mode'] = 9
        trip.loc[(trip['mode_type_imputed']==9) & (trip['mode']==0), 'mode'] = 9
        
        if 'is_transit' in trip.columns:
            trip.loc[(trip['is_transit']==1) & (trip['mode']==0), 'mode'] = 6
            DRIVE_ACCESS_CODES = [3,4,9,10]
            trip.loc[(trip['mode']==6) & 
                     ((trip['access_mode_type'].isin(DRIVE_ACCESS_CODES)) | (trip['egress_mode_type'].isin(DRIVE_ACCESS_CODES))), 'mode'] = 7
        else:
            DRIVE_ACCESS_CODES = [5,6,7]
    #         TRANSIT_MODES = [5, 8]
            TRANSIT_MODES = [5]
            trip.loc[(trip['mode_type_imputed'].isin(TRANSIT_MODES)) & (trip['mode']==0), 'mode'] = 6
    #         trip.loc[(trip['mode_type_imputed']==8) & (trip['mode_1']==21), 'mode'] = 5 #make vanpool HOV3
            trip.loc[(trip['mode_type_imputed']==8), 'mode'] = 5 #make both shuttle and vanpool HOV3
            trip.loc[(trip['mode_type_imputed']==13) & (trip['mode_1']==41) & (trip['mode']==0), 'mode'] = 6
            
            trip.loc[(trip['mode']==6) & 
                     ((trip['bus_access'].isin(DRIVE_ACCESS_CODES)) | (trip['bus_egress'].isin(DRIVE_ACCESS_CODES)) |
                      (trip['rail_access'].isin(DRIVE_ACCESS_CODES)) | (trip['rail_access'].isin(DRIVE_ACCESS_CODES))), 'mode'] = 7
        trip.loc[(trip['mode_type_imputed']==6) & (trip['mode']==0), 'mode'] = 8 #Schoolbus
        
        #0-none 1-fullnetwork 2-notoll 3-bus 4-lrt 5-prem 6-bart 7-ferry
        TRN_MODES_PROCESSED = [6,7]
        trip['path'] = 0
        trip.loc[(trip['mode_type_imputed']==3) & (trip['path']==0), 'path'] = 1
        trip.loc[(trip['mode'].isin(TRN_MODES_PROCESSED)) & (trip['path']==0), 'path'] = 3
        trip.loc[(trip['mode'].isin(TRN_MODES_PROCESSED)) & 
                 ((trip['mode_1'].isin([32])) | (trip['mode_2'].isin([32])) | (trip['mode_3'].isin([32])) | (trip['mode_4'].isin([32]))) &
                 (trip['path']==3), 'path'] = 7
        trip.loc[(trip['mode'].isin(TRN_MODES_PROCESSED)) & 
                 ((trip['mode_1'].isin([30])) | (trip['mode_2'].isin([30])) | (trip['mode_3'].isin([30])) | (trip['mode_4'].isin([30]))) &
                 (trip['path']==3), 'path'] = 6
        trip.loc[(trip['mode'].isin(TRN_MODES_PROCESSED)) & 
                 ((trip['mode_1'].isin([41,42,55])) | (trip['mode_2'].isin([41,42,55])) | (trip['mode_3'].isin([41,42,55])) | (trip['mode_4'].isin([41,42,55]))) &
                 (trip['path']==3), 'path'] = 5
        trip.loc[(trip['mode'].isin(TRN_MODES_PROCESSED)) & 
                 ((trip['mode_1'].isin([39,68])) | (trip['mode_2'].isin([39,68])) | (trip['mode_3'].isin([39,68])) | (trip['mode_4'].isin([39,68]))) &
                 (trip['path']==3), 'path'] = 4
        
        trip['dorp'] = 3
        trip.loc[trip['mode'].isin([3,4,5]), 'dorp'] = 9 #assign missing code for all car trips
        trip.loc[(trip['mode'].isin([3,4,5])) & (trip['driver']==1), 'dorp'] = 1
        trip.loc[(trip['mode'].isin([3,4,5])) & (trip['driver']==2), 'dorp'] = 2
        trip.loc[(trip['mode']==9) & (trip['num_travelers']==1), 'dorp'] = 11
        trip.loc[(trip['mode']==9) & (trip['num_travelers']==2), 'dorp'] = 12
        trip.loc[(trip['mode']==9) & (trip['num_travelers']>2), 'dorp'] = 13
        
        trip['depart_hour'] = pd.to_datetime(trip['depart_time_imputed']).dt.hour
        trip['depart_minute'] = pd.to_datetime(trip['depart_time_imputed']).dt.minute
        
#         trip['depart_hour'] = trip['depart_time_imputed'].str.split(expand=True)[1].str.split(':',expand=True)[0].astype(int)
#         trip['depart_minute'] = trip['depart_time_imputed'].str.split(expand=True)[1].str.split(':',expand=True)[1].astype(int)
        
        trip['deptm'] = trip['depart_hour']*100 + trip['depart_minute']
        if 'arrive_hour' not in trip.columns:
            trip['arrive_hour'] = pd.to_datetime(trip['arrive_time']).dt.hour
            trip['arrive_minute'] = pd.to_datetime(trip['arrive_time']).dt.minute
#             trip['arrive_hour'] = trip['arrive_time'].str.split(expand=True)[1].str.split(':',expand=True)[0].astype(int)
#             trip['arrive_minute'] = trip['arrive_time'].str.split(expand=True)[1].str.split(':',expand=True)[1].astype(int)
        trip['arrtm'] = trip['arrive_hour']*100 + trip['arrive_minute']
        
        trip['oxcord'] = trip['o_lon']
        trip['oycord'] = trip['o_lat']
        trip['dxcord'] = trip['d_lon']
        trip['dycord'] = trip['d_lat']
        
        trip = trip.rename(columns={'otaz':'otaz_tmp', 'dtaz':'dtaz_tmp',
                                  'opcl':'opcl_tmp', 'dpcl':'dpcl_tmp'})
        trip.loc[trip['o_county_fips'].isin(COUNTY_FIPS), 'otaz'] = trip.loc[trip['o_county_fips'].isin(COUNTY_FIPS), 'otaz_tmp']
        trip.loc[trip['o_county_fips'].isin(COUNTY_FIPS), 'opcl'] = trip.loc[trip['o_county_fips'].isin(COUNTY_FIPS), 'opcl_tmp']
        trip.loc[trip['d_county_fips'].isin(COUNTY_FIPS), 'dtaz'] = trip.loc[trip['d_county_fips'].isin(COUNTY_FIPS), 'dtaz_tmp']
        trip.loc[trip['d_county_fips'].isin(COUNTY_FIPS), 'dpcl'] = trip.loc[trip['d_county_fips'].isin(COUNTY_FIPS), 'dpcl_tmp']
        trip.loc[pd.isnull(trip['otaz']), 'otaz'] = -1
        trip.loc[pd.isnull(trip['dtaz']), 'dtaz'] = -1
        trip.loc[pd.isnull(trip['opcl']), 'opcl'] = -1
        trip.loc[pd.isnull(trip['dpcl']), 'dpcl'] = -1
        trip.loc[pd.isnull(trip['oxcord']), 'oxcord'] = -1.0
        trip.loc[pd.isnull(trip['oycord']), 'oycord'] = -1.0
        trip.loc[pd.isnull(trip['dxcord']), 'dxcord'] = -1.0
        trip.loc[pd.isnull(trip['dycord']), 'dycord'] = -1.0
        
        int_cols = ['opcl','otaz','dpcl','dtaz','opurp','dpurp'] 
        trip[int_cols] = trip[int_cols].astype(int)
          
        out_colnames = ['hhno','pno','tripno','dow','opurp','dpurp','opcl','otaz','dpcl','dtaz','mode','path','dorp',
                        'deptm','arrtm','oxcord','oycord','dxcord','dycord',
                        'daywt_alladult_wkday','daywt_alladult_7day','mode_type_imputed']
        trip = trip[out_colnames]
        trip = trip.sort_values(['hhno','pno','tripno'])
        trip.to_csv(outtripfilename, sep=OUT_SEP, index=False)
        
        print('Trip file processing finished: {}'.format(datetime.datetime.now()))               
        logfile.write('Trip file processing finished: ' + str(datetime.datetime.now()) + '\n')
    
    
        logfile.write('\n')                
        logfile.write('Reformat survey program finished: ' + str(datetime.datetime.now()) + '\n')
        logfile.close()

        print('Reformat survey program finished: {}'.format(datetime.datetime.now()))
        
        
        
        
        
        