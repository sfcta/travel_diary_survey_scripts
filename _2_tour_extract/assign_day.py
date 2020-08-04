'''
Created on May 5, 2020

@author: bsana
'''

import pandas as pd
from os.path import join

BASE_DIR = r'..\..\MTC-SFCTA2018\Processing_20200228\2_tour_extract'
RAW_DIR = r'..\..\MTC-SFCTA2018\Processing_20200228\spatial_join'
DOW_LOOKUP = {1:'mon',2:'tue',3:'wed',4:'thu',5:'fri',6:'sat',7:'sun'}

# WT_CAP = 10000
# out_dir = 'wt_wkday_cap'
WT_CAP = None
out_dir = 'wt_wkday'
wt_col = 'wt_alladult_wkday'
wt_num = 'nwkdaywts_complete'
wt_days = [1,2,3,4]

# out_dir = 'wt_7day'
# wt_col = 'wt_alladult_7day'
# wt_num = 'n7daywts_complete'
# wt_days = [1,2,3,4,5,6,7]

# function to link drive transit trips
def link_dt(df):
    dtrn_df = df.loc[df['dpurp']==10,]
    dtrn_df.loc[:,'tseg'] += 1
    dtrn_df = dtrn_df[['hhno','pno','day','tour','half','tseg','otaz','opurp']]
    dtrn_df  = dtrn_df.rename(columns={'otaz':'otaz_drive','opurp':'opurp_drive'})
    df = df.loc[df['dpurp']!=10,]
    df = df.merge(dtrn_df, on=['hhno','pno','day','tour','half','tseg'], how='left')
    df.loc[df['opurp']==10, 'otaz'] = df.loc[df['opurp']==10, 'otaz_drive']
    df.loc[df['opurp']==10, 'mode'] = 7
    df.loc[df['opurp']==10, 'opurp'] = df.loc[df['opurp']==10, 'opurp_drive']
    return df

# read in week files
tour = pd.read_csv(join(BASE_DIR, 'survey2018_tourx.dat'), sep=' ')
tour_cols = tour.columns

trip = pd.read_csv(join(BASE_DIR, 'survey2018_tripx.dat'), sep=' ')
trip_cols = trip.columns

pday = pd.read_csv(join(BASE_DIR, 'survey2018_pdayx.dat'), sep=' ')
pday_cols = pday.columns

per = pd.read_csv(join(BASE_DIR, 'survey2018_precx.dat'), sep=' ')
per_cols = per.columns

hh = pd.read_csv(join(BASE_DIR, 'survey2018_hrecx.dat'), sep=' ')
hh_cols = hh.columns

# read in raw person file for weight info
raw_per = pd.read_csv(join(RAW_DIR, 'ex_person_wZones.csv'))
raw_per = raw_per[['hh_id','person_num','wt_alladult_wkday','wt_alladult_7day',
                   'nwkdaywts_complete','n7daywts_complete',
                   'mon_complete','tue_complete','wed_complete','thu_complete',
                   'fri_complete','sat_complete','sun_complete']]
raw_per = raw_per.rename(columns={'hh_id':'hhno', 'person_num':'pno'})
if WT_CAP != None:
    raw_per.loc[raw_per[wt_col]>WT_CAP, wt_col] = WT_CAP
raw_per['weight'] = raw_per[wt_col]/raw_per[wt_num]

# read in raw trip file for dow info
raw_trips = pd.read_csv(join(RAW_DIR, 'ex_trip_wZones.csv'))
raw_trips = raw_trips[['hh_id','person_num','trip_num','travel_date_dow',
                       'o_purpose_category_imputed','d_purpose_category_imputed']]
raw_trips.columns = ['hhno','pno','tsvid','dow','opurp','dpurp']
trip = trip.merge(raw_trips[['hhno','pno','tsvid','dow']], how='left')
trip['count'] = 1
# derive dow with maximum trips in a given tour
tour_dow = trip[['hhno','pno','tour','dow','count']].groupby(['hhno','pno','tour','dow']).sum().reset_index() 
tour_dow = tour_dow.sort_values(['hhno','pno','tour','count'], 
                                ascending=[True, True, True, False])
tour_dow = tour_dow.drop_duplicates(['hhno','pno','tour'])
tour_dow = tour_dow[['hhno','pno','tour','dow']]

# assign the dow with max trips to the tour day
tour = tour.merge(tour_dow, how='left')
tour['day'] = tour['dow'].astype(int)
# assign tour weight
tour = tour.merge(raw_per[['hhno','pno','weight']], how='left')
tour['toexpfac'] = 0
tour.loc[tour['day'].isin(wt_days), 'toexpfac'] = tour.loc[tour['day'].isin(wt_days), 'weight']
tour['toexpfac'] = tour['toexpfac'].fillna(0)
tour = tour[tour_cols]
tour.to_csv(join(BASE_DIR, out_dir, 'survey2018_tourx.dat'), sep=' ', index=False)

# assign trip dow
trip = trip.drop(['day','dow'], axis=1)
trip = trip.merge(tour_dow, how='left')
trip['day'] = trip['dow'].astype(int)
# assign trip weight
trip = trip.merge(raw_per[['hhno','pno','weight']], how='left')
trip['trexpfac'] = 0
trip.loc[trip['day'].isin(wt_days), 'trexpfac'] = trip.loc[trip['day'].isin(wt_days), 'weight']
trip['trexpfac'] = trip['trexpfac'].fillna(0)
trip = trip[trip_cols]
trip.to_csv(join(BASE_DIR, out_dir, 'survey2018_tripx.dat'), sep=' ', index=False)

# create pday file
pday_out = pd.DataFrame()
for key, val in DOW_LOOKUP.items():
    df = raw_per.loc[raw_per[val+'_complete']==1 , ['hhno','pno']]
    df['day'] = key
    pday_out = pday_out.append(df)

# calculate tours
tour['pdpurp2'] = tour['pdpurp']
tour.loc[tour['parent']>0, 'pdpurp2'] = 8
tour['count'] = 1
tour_agg = tour[['hhno','pno','day','pdpurp2','count']].groupby(['hhno','pno','day','pdpurp2']).sum().reset_index()
tour_agg = tour_agg[tour_agg['pdpurp2'].isin(range(1,9))]
tour_agg = tour_agg.pivot_table(index=['hhno','pno','day'], columns='pdpurp2', values='count').reset_index()
tour_agg.columns = ['hhno','pno','day','wktours','sctours','estours','pbtours',
                    'shtours','mltours','sotours','wbtours']
pday_out = pday_out.merge(tour_agg, how='left')
pday_out = pday_out.fillna(0)

# calculate stops
linked_trips = link_dt(trip)
tripsh1 = linked_trips[['hhno','pno','day','tour','half','tseg']].groupby(['hhno','pno','day','tour','half']).max().reset_index()
tripsh1 = tripsh1[tripsh1['half']==1]
tripsh1['rm_flag'] = 1
tripsh2 = linked_trips[['hhno','pno','day','tour','half','tseg']].groupby(['hhno','pno','day','tour','half']).min().reset_index()
tripsh2 = tripsh2[tripsh2['half']==2]
tripsh2['rm_flag'] = 1
rm_trips = pd.concat([tripsh1, tripsh2])
linked_trips = linked_trips.merge(rm_trips, how='left')
linked_trips['rm_flag'] = linked_trips['rm_flag'].fillna(0)
linked_trips = linked_trips[linked_trips['rm_flag']==0]

linked_trips['count'] = 1
trip_agg = linked_trips[['hhno','pno','day','dpurp','count']].groupby(['hhno','pno','day','dpurp']).sum().reset_index()
trip_agg = trip_agg[trip_agg['dpurp'].isin(range(1,9))]
trip_agg = trip_agg.pivot_table(index=['hhno','pno','day'], columns='dpurp', values='count').reset_index()
trip_agg.columns = ['hhno','pno','day','wkstops','scstops','esstops','pbstops',
                    'shstops','mlstops','sostops']
pday_out = pday_out.merge(trip_agg, how='left')
pday_out = pday_out.fillna(0)

# assign beghom and endhom
raw_trips = raw_trips.rename(columns={'dow':'day'})
first_trips = raw_trips[['hhno','pno','day','tsvid']].groupby(['hhno','pno','day']).min().reset_index()
first_trips = first_trips.rename(columns={'tsvid':'trip_first'})
first_trips = trip.merge(first_trips, how='left', on=['hhno','pno','day'])
first_trips = first_trips[first_trips['tsvid']==first_trips['trip_first']]
first_trips = first_trips[first_trips['opurp']==0]
first_trips['beghom'] = 1
first_trips = first_trips[['hhno','pno','day','beghom']]
pday_out = pday_out.merge(first_trips, how='left')

last_trips = raw_trips[['hhno','pno','day','tsvid']].groupby(['hhno','pno','day']).max().reset_index()
last_trips = last_trips.rename(columns={'tsvid':'trip_last'})
last_trips = trip.merge(last_trips, how='left', on=['hhno','pno','day'])
last_trips = last_trips[last_trips['tsvid']==last_trips['trip_last']]
last_trips = last_trips[last_trips['dpurp']==0]
last_trips['endhom'] = 1
last_trips = last_trips[['hhno','pno','day','endhom']]
pday_out = pday_out.merge(last_trips, how='left')

pday_out['hbtours'] = pday_out[['wktours','sctours','estours','pbtours','shtours','mltours','sotours']].sum(axis=1)
pday_out['uwtours'] = pday_out['wktours']
pday_out['retours'] = 0
pday_out['metours'] = 0
pday_out['restops'] = 0
pday_out['mestops'] = 0

pday_out = pday_out.merge(raw_per[['hhno','pno','weight']], how='left')
pday_out['pdexpfac'] = 0
pday_out.loc[pday_out['day'].isin(wt_days), 'pdexpfac'] = pday_out.loc[pday_out['day'].isin(wt_days), 'weight']
pday_out = pday_out.fillna(0)
pday_out = pday_out[pday_cols]
# pday_out[:-1] = pday_out[:-1].astype(int)
pday_out.to_csv(join(BASE_DIR, out_dir, 'survey2018_pdayx.dat'), sep=' ', index=False)

# assign person weight
per = per.merge(raw_per[['hhno','pno',wt_col]], how='left')
per['psexpfac'] = per[wt_col]
per['psexpfac'] = per['psexpfac'].fillna(0)
per = per[per_cols]
per.to_csv(join(BASE_DIR, out_dir, 'survey2018_precx.dat'), sep=' ', index=False)
# no changes to hh file
hh.to_csv(join(BASE_DIR, out_dir, 'survey2018_hrecx.dat'), sep=' ', index=False)


