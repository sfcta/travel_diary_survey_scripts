import sys, os
import numpy as np
import pandas as pd
from survey import Survey

TRIP_REQS = [
    'o_purpose_category_complete',
    'd_purpose_category_complete',
    'mode_type_complete',
    'depart_hour_complete',
    'depart_minute_complete',
    'depart_seconds_complete',
    'arrive_hour_complete',
    'arrive_minute_complete',
    'arrive_second_complete',
    'o_lat_complete',
    'o_lon_complete',
    'd_lat_complete',
    'd_lon_complete'
]
DAY_REQS = ['telecommute_time_complete']

HH_REQS = [
    'num_workers_complete',
    'num_adults_complete',
    'num_kids_complete', #'income_detailed','income_broad',
    'num_workers_complete',
    'num_vehicles_complete',
    'home_lat_complete',
    'home_lon_complete'
]
PERSON_REQS = [
    'race_complete',
    'ethnicity_complete',
    'gender_complete',
    'age_complete',
    'employment_complete',
    'telework_complete',
    'school_loc_complete',
    'work_park_complete'
]

def flag_trip_item_complete(trip):
    pass
def flag_day_item_compete(day):
    pass
def flag_person_item_complete(person):
    pass
def flag_hh_item_complete(hh):
    pass

def flag_complete_trips(trip, trip_reqs):
    complete = 1
    for c in trip_reqs:
        complete = complete * trip[c]
    trip['sfcta_is_complete'] = complete
    return trip

def flag_complete_days(day, trip, day_reqs):
    ## apply the day flags
    day.drop(columns=['sfcta_num_trips','sfcta_day_trips_complete'], inplace=True)
    
    # recalc day trips complete based on the trip completeness status above.
    tmp = (trip
           .groupby(['hh_id','person_id','day_num'])
           .agg({'sfcta_is_complete':'sum','trip_id':'count'})
           .rename(columns={'sfcta_is_complete':'sfcta_num_trips'}))
    tmp['sfcta_day_trips_complete'] = (tmp['sfcta_num_trips'].eq(tmp['trip_id'])*1)
    day = pd.merge(day, tmp[['sfcta_num_trips','sfcta_day_trips_complete']], left_on=['hh_id','person_id','day_num'], right_index=True, how='left')

    # correct for no-travel days
    day['sfcta_num_trips'] = day['sfcta_num_trips'].fillna(0)
    day.loc[day['sfcta_num_trips'].eq(0) & day['no_travel_1'].eq(0),'sfcta_day_trips_complete'] = 1
    day['sfcta_day_trips_complete'] = day['sfcta_day_trips_complete'].fillna(0)
    
    complete = 1
    for c in day_reqs:
        complete = complete * day[c]
    
    day['sfcta_day_complete'] = (complete * day['sfcta_day_trips_complete']) * 1
    return day

def flag_complete_persons(person, day, person_reqs, min_concurrent_days):
    person.drop(columns=['sfcta_num_days_complete'], inplace=True)
    # roll up to person level
    person = pd.merge(person,
                      day.groupby('person_id', as_index=False)
                         .agg({'sfcta_day_complete':'sum'})
                         .rename(columns={'sfcta_day_complete':'sfcta_num_days_complete'}),
                      how='left')
    complete = 1
    for c in person_reqs:
        complete = complete * person[c]
    
    ## apply the minimum concurrent days req
    person['sfcta_person_days_complete'] = (((person['diary_platform'].isin(['browser','call']) | person['has_proxy'].eq(1)) & person['sfcta_num_days_complete'].ge(1)) |
                                                    (person['diary_platform'].eq('rmove') & person['sfcta_num_days_complete'].ge(max(1,min_concurrent_days)))) * 1
    person['sfcta_person_survey_complete'] = complete
    person['sfcta_person_complete'] = (person['sfcta_person_survey_complete'].eq(1) & person['sfcta_person_days_complete'].eq(1))*1
    return person

def flag_concurrent_days(hh, person, day, min_concurrent_days, min_concurrent_weighted_days):
    hh.drop(columns=['num_concurrent_non_proxy_days','num_concurrent_total_days',
                     'num_concurrent_weighted_weekdays','num_concurrent_weighted_days'], inplace=True)
    # concurrent days
    day = pd.merge(day, hh[['hh_id','diary_platform']])
    day = pd.merge(day, person[['person_id','has_proxy']])
    
    concurrent_day = day.pivot_table(index=['hh_id','diary_platform'], 
                                     columns=['has_proxy','travel_dow'], 
                                     values='sfcta_day_complete', 
                                     aggfunc='sum').fillna(0).astype(int).reset_index().set_index('hh_id')
    concurrent_weighted_day = day.pivot_table(index=['hh_id','diary_platform'], 
                                     columns=['has_weight','travel_dow'], 
                                     values='sfcta_day_complete', 
                                     aggfunc='sum').fillna(0).astype(int).reset_index().set_index('hh_id')
    
    hh_size = (person.groupby('hh_id')
                     .agg({'person_id':'nunique','has_proxy':'sum'})
                     .rename(columns={'person_id':'persons','has_proxy':'proxy_persons'})
              )
    hh_size['non_proxy_persons'] = hh_size['persons'] - hh_size['proxy_persons']
    concurrent_day['num_concurrent_non_proxy_days'] = (concurrent_day[0].eq(hh_size['non_proxy_persons'], axis=0)*1).sum(axis=1)
    concurrent_day['num_concurrent_total_days'] = ((concurrent_day[0]+concurrent_day[1]).eq(hh_size['persons'], axis=0)*1).sum(axis=1)
    concurrent_weighted_day['num_concurrent_weighted_weekdays'] = ((concurrent_weighted_day[1].loc[:,2:4]).eq(hh_size['persons'], axis=0)*1).sum(axis=1)
    concurrent_weighted_day['num_concurrent_weighted_days'] = ((concurrent_weighted_day[1]).eq(hh_size['persons'], axis=0)*1).sum(axis=1)
    
    j = pd.DataFrame(concurrent_day[['num_concurrent_non_proxy_days','num_concurrent_total_days']])
    j.columns=['num_concurrent_non_proxy_days','num_concurrent_total_days']
    k = pd.DataFrame(concurrent_weighted_day[['num_concurrent_weighted_weekdays','num_concurrent_weighted_days']])
    k.columns=['num_concurrent_weighted_weekdays','num_concurrent_weighted_days']
    jk = pd.merge(j, k, left_index=True, right_index=True)
    hh = pd.merge(hh, jk, left_on='hh_id', right_index=True)
    hh.set_index('hh_id', inplace=True)
    hh['sfcta_hh_concurrent_complete'] = ((concurrent_day['diary_platform'].eq('rmove') & 
                                           concurrent_day['num_concurrent_non_proxy_days'].ge(min_concurrent_days) & 
                                           concurrent_day['num_concurrent_total_days'].ge(1)) | 
                                          (concurrent_day['diary_platform'].isin(['call','browser']) &
                                           concurrent_day['num_concurrent_non_proxy_days'].ge(1) & 
                                           concurrent_day['num_concurrent_total_days'].ge(1)))* 1
    hh['sfcta_hh_concurrent_weighted_complete'] = concurrent_weighted_day['num_concurrent_weighted_weekdays'].ge(min_concurrent_weighted_days)*1
    hh.reset_index(inplace=True)
    return hh
    
def flag_complete_hhs(hh, person, hh_reqs):
    hh.drop(columns=['sfcta_num_persons_complete','sfcta_hh_persons_complete'], inplace=True)
    
    # person rollup to hh
    tmp = (person.groupby('hh_id', as_index=False)
                 .agg({'sfcta_person_complete':'sum', 'person_id':'count','sfcta_bipoc_flag':'sum'})
                 .rename(columns={'sfcta_person_complete':'sfcta_num_persons_complete',
                                  'person_id':'persons'})
           )
    tmp['sfcta_hh_persons_complete'] = (tmp['sfcta_num_persons_complete'].eq(tmp['persons']))*1
    hh = pd.merge(hh, tmp[['hh_id','sfcta_num_persons_complete','sfcta_hh_persons_complete']],
                  on='hh_id', how='left')
    
    complete = 1
    for c in hh_reqs:
        complete = complete * hh[c]
    
    hh['sfcta_hh_survey_complete'] = complete
    hh['sfcta_hh_complete'] = (hh['sfcta_hh_survey_complete'].eq(1) & 
                               hh['sfcta_hh_persons_complete'].eq(1) &
                               hh['sfcta_hh_concurrent_complete'].eq(1) & 
                               hh['sfcta_hh_concurrent_weighted_complete'].eq(1))*1
    return hh
    
def flag_complete(survey, 
                  hh_unit='family', 
                  hh_reqs=HH_REQS, 
                  person_reqs=PERSON_REQS, 
                  day_reqs=DAY_REQS, 
                  trip_reqs=TRIP_REQS, 
                  concurrent_days=1, 
                  concurrent_weighted_days=1):
    '''
    takes a Survey that has already been flagged for item completeness and 
    applies overall completeness criteria
    
    survey
    hh_unit: 'family' or 'household'
    hh_reqs: list of hh item completeness flag fields
    person_reqs: list of person item completeness flag fields
    day_reqs: list of day item completeness flag fields
    trip_reqs: list of trip item completeness flag fields
    concurrent_days: number of required concurrent complete days for adult hh members.  Proxy reported children must also have one concurrent day
    concurrent_weighted_days: number of required concurrent complete days for adult hh members.  Proxy reported children must also have one concurrent day
    '''
    if hh_unit == 'family':
        person = survey.person.loc[survey.person['relationship'].le(5)]
        hh = survey.hh.loc[survey.hh['hh_id'].isin(person['hh_id'])]
        day = survey.day.loc[survey.day['person_id'].isin(person['person_id'])]
        trip = survey.trip.loc[survey.trip['person_id'].isin(person['person_id'])]
    else:
        person = survey.person.copy()
        hh = survey.hh.copy()
        day = survey.day.copy()
        trip = survey.trip.copy()

    ## apply the trip flags
    trip = flag_complete_trips(trip, trip_reqs)

    ## apply the day flags
    day = flag_complete_days(day, trip, day_reqs)

    ## apply the person flags
    person = flag_complete_persons(person, day, person_reqs, concurrent_days)

    ## flag household concurrent days
    hh = flag_concurrent_days(hh, person, day, concurrent_days, concurrent_weighted_days)

    ## apply the household flags
    hh = flag_complete_hhs(hh, person, hh_reqs)
    
    survey.hh = hh
    survey.person = person
    survey.day = day
    survey.trip = trip
    
    return survey