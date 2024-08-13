import sys, os
import numpy as np
import pandas as pd

if __name__=='__main__':
    indir = r'Q:\Data\Surveys\HouseholdSurveys\MTC-SFCTA2022\Deliverable_20240329\BATS Unweighted Dataset'
    outdir = r'Q:\Data\Surveys\HouseholdSurveys\MTC-SFCTA2022\Processed_20240329'
    
    HOUSEHOLD = 'hh.csv'
    PERSON = 'person.csv'
    DAY = 'day.csv'
    TRIP = 'trip.csv'
    LOCATION = 'location.csv'
    VEHICLE = 'vehicle.csv'
    
    household = pd.read_csv(os.path.join(indir,HOUSEHOLD))
    person = pd.read_csv(os.path.join(indir,PERSON))
    day = pd.read_csv(os.path.join(indir,DAY))
    trip = pd.read_csv(os.path.join(indir,TRIP))
    location = pd.read_csv(os.path.join(indir,LOCATION))
    vehicle = pd.read_csv(os.path.join(indir,VEHICLE))
    
    print('households: {}'.format(len(household)))
    print('persons: {}'.format(len(person)))
    print('days: {}'.format(len(day)))
    print('trips: {}'.format(len(trip)))
    print('locations: {}'.format(len(location)))
    print('vehicles: {}'.format(len(vehicle)))
    
    if 'depart_seconds' in trip.columns:
        trip.rename(columns={'depart_seconds':'depart_second'}, inplace=True)
    trip['depart_time'] = trip.apply(lambda x: '{:02d}:{:02d}:{:02d}'.format(x['depart_hour'], x['depart_minute'], x['depart_second']), axis=1)
    trip['arrive_time'] = trip.apply(lambda x: '{:02d}:{:02d}:{:02d}'.format(x['arrive_hour'], x['arrive_minute'], x['arrive_second']), axis=1)
    
    person['person_id_sfcta'] = person.apply(lambda x: '{:d}{:02d}'.format(x['hh_id'],x['person_num']), axis=1).astype('int64')
    trip['person_id_sfcta'] = trip.apply(lambda x: '{:d}{:02d}'.format(x['hh_id'],x['person_num']), axis=1).astype('int64')
    trip['trip_id_sfcta'] = trip.apply(lambda x: '{:d}{:03d}'.format(x['person_id_sfcta'],x['trip_num']), axis=1).astype('int64')
    location = pd.merge(trip[['person_id','person_id_sfcta','trip_id','trip_id_sfcta']], location, on='trip_id', how='right')
    
    print('households: {}'.format(len(household)))
    print('persons: {}'.format(len(person)))
    print('days: {}'.format(len(day)))
    print('trips: {}'.format(len(trip)))
    print('locations: {}'.format(len(location)))
    print('vehicles: {}'.format(len(vehicle)))
    
    household.to_csv(os.path.join(outdir,HOUSEHOLD), index=False)
    person.to_csv(os.path.join(outdir,PERSON), index=False)
    day.to_csv(os.path.join(outdir,DAY), index=False)
    trip.to_csv(os.path.join(outdir, TRIP), index=False)
    location.to_csv(os.path.join(outdir, LOCATION), index=False)
    vehicle.to_csv(os.path.join(outdir, VEHICLE), index=False)