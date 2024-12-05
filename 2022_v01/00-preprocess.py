"""
copy all CSV files from raw_dir to 00-preprocess dir with minor changes:
trip: add columns: depart_time, arrive_time
location: add person_id from trip table
"""

import argparse
import tomllib
from pathlib import Path
from shutil import copy2

import numpy as np
import pandas as pd
import geopandas as gpd
import datetime as dt
from shapely.geometry import Point

def preprocess(config):
    raw_dir = Path(config["raw"]["dir"])
    preprocess_dir = Path(config["00-preprocess"]["dir"])
    preprocess_dir.mkdir(exist_ok=True)

    # read raw files
    hh = pd.read_csv(raw_dir / config['hh_filename'])
    person = pd.read_csv(raw_dir / config['person_filename'])
    day = pd.read_csv(raw_dir / config['day_filename'])
    trip = pd.read_csv(raw_dir / config["trip_filename"])
    
    if "depart_seconds" in trip.columns:
        trip.rename(columns={"depart_seconds": "depart_second"}, inplace=True)
    
    location = pd.read_csv(raw_dir / config["location_filename"])
    
    # do processing
    location = preprocess_location(trip, location)
    trip, location = split_trips(hh, person, day, trip, location)
    trip = preprocess_trip(trip)
    
    # write processed files
    trip.to_csv(preprocess_dir / config["trip_filename"], index=False)
    location.to_csv(preprocess_dir / config["location_filename"], index=False)

    # copying the unchanged files too for ease of use / easy backward compatibility;
    # not sure for logical clarity if we should copy files that are not changed
    day_filename = config["day_filename"]
    hh_filename = config["hh_filename"]
    person_filename = config["person_filename"]
    vehicle_filename = config["vehicle_filename"]
    copy2(raw_dir / day_filename, preprocess_dir / day_filename)
    copy2(raw_dir / hh_filename, preprocess_dir / hh_filename)
    copy2(raw_dir / person_filename, preprocess_dir / person_filename)
    copy2(raw_dir / vehicle_filename, preprocess_dir / vehicle_filename)
    return

def get_location_purpose(location, threshold=5280.0 / 4.0):
    other_people = list(range(1, location['person_num'])) + list(range(location['person_num']+1, 11))
    work_cols = ['dist_work'] + ['dist_work_{}'.format(i) for i in other_people]
    school_cols = [] #['dist_school'] + ['dist_school_{}'.format(i) for i in other_people]
    second_home_cols = [] #['dist_second_home'] + ['dist_second_home_{}'.format(i) for i in other_people]
    cols = ['dist_home'] + work_cols + school_cols + second_home_cols

    c, d = None, threshold
    for col in cols:
        if location[col] < d:
            c = col
            d = location[col]

    if d == threshold:
        return -1 # unkown
    if c == 'dist_home':
        return 1 # home
    if c == 'dist_work':
        return 2 # work
    if c == 'dist_school':
        return 4 # school
    if c == 'dist_second_home':
        return 12 # overnight
    return 3

def get_trip(location, orig_trip, day):
    trip = orig_trip.copy()
    span = (location.groupby('trip_id')
                    .agg({'duration':'sum',
                          'dist_next':'sum',
                          })
                    .rename(columns={'duration':'minutes',
                                     'dist_next':'feet'})
           ).iloc[0]
    depart = location.iloc[0]
    arrive = location.iloc[-1]

    # get the date, using 3am-3am
    date = str((depart['timestamp_local'] - dt.timedelta(hours=3)).date())
    
    try:
        _day = day.loc[day['person_id'].eq(depart['person_id']) & day['travel_date'].eq(date)].iloc[0]
    except:
        return None
        #_day = day.loc[day['person_id'].eq(depart['person_id'])].iloc[-1]
        #_day['day_num'] = _day['day_num'] + 1
        #_day['day_id'] = _day['day_id'] + 1
        
    trip['travel_date'] = date
    trip['day_id'] = _day['day_id']
    trip['day_num'] = _day['day_num']
    trip['depart_date'] = date
    trip['depart_hour'] = depart['timestamp_local'].hour
    trip['depart_minute'] = depart['timestamp_local'].minute
    trip['depart_second'] = depart['timestamp_local'].second
    trip['depart_dow'] = depart['timestamp_local'].weekday()
    trip['o_lon'] = depart['lon']
    trip['o_lat'] = depart['lat']
    
    trip['arrive_date'] = str(arrive['timestamp_local'].date())
    trip['arrive_hour'] = arrive['timestamp_local'].hour
    trip['arrive_minute'] = arrive['timestamp_local'].minute
    trip['arrive_second'] = arrive['timestamp_local'].second
    trip['arrive_dow'] = arrive['timestamp_local'].weekday()
    trip['d_lon'] = arrive['lon']
    trip['d_lat'] = arrive['lat']
    
    trip['distance_meters'] = span['feet'] * 0.3048
    trip['distance_miles'] = span['feet'] / 5280.0
    trip['duration_seconds'] = span['minutes'] * 60.0
    trip['duration_minutes'] = span['minutes']
    
    if span['minutes'] > 0:
        trip['speed_mph'] = (span['feet'] / span['minutes']) * (60.0 / 5280.0)
    else:
        trip['speed_mhp'] = 0
    
    opurp = get_location_purpose(depart)
    dpurp = get_location_purpose(arrive)
    
    if opurp != -1:
        trip['o_purpose_category']
    if dpurp != -1:
        trip['d_purpose_category']

    return trip


def preprocess_trip(trip):
    '''
    calculate `depart_time` and `arrive_time`
    '''
    print("trip raw len:", len(trip))
    trip["depart_time"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["depart_hour"], x["depart_minute"], x["depart_second"]
        ),
        axis=1,
    )
    trip["arrive_time"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["arrive_hour"], x["arrive_minute"], x["arrive_second"]
        ),
        axis=1,
    )
    print("trip preprocessed len:", len(trip))
    return trip


def preprocess_location(trip, location):
    '''
    Attach household, person, and trip attributes to location.  Calculate 
    point-to-next-point distances and durations, and calculate distances 
    to points-of-interest.  
    '''
    print("location raw len:", len(location))
    # attach hh, person, and trip attributes
    loc = pd.merge(
        trip[['hh_id','person_id','person_num','trip_id','trip_num']],
        location,
        on="trip_id",
        how="right",
    )
    
    # calculate datetime attributes
    d = pd.DatetimeIndex(pd.to_datetime(loc['collect_time'], format='ISO8601'))
    d = d.tz_convert('US/Pacific')
    loc['timestamp_local'] = d
    #loc['time'] = loc['timestamp_local'].map(lambda x: '{:02d}{:02d}'.format(x.hour, x.minute))
    #loc['dow'] = loc['timestamp_local'].map(lambda x: x.weekday())
    
    loc = loc.sort_values(['hh_id','person_num','timestamp_local']).reset_index()
    
    # calculate next-point distance and durations
    loc = gpd.GeoDataFrame(data=loc, 
                           geometry=loc.apply(lambda x: Point(x['lon'], x['lat']), axis=1), 
                           crs='epsg:4326').to_crs('epsg:2227')
    shift = loc.shift(-1)
    loc.loc[loc['trip_id'].eq(shift['trip_id']), 'dist_next'] = loc.distance(shift)
    loc.loc[loc['trip_id'].eq(shift['trip_id']), 'duration'] = (shift['timestamp_local'] - loc['timestamp_local']).map(lambda x: x.total_seconds() / 60.0)
    loc['speed_next'] = (loc['dist_next'] / 5280.0) / (loc['duration'] / 60.0)    
    print("location preprocessed len:", len(loc))    
    return loc
    
def append_distances(hh, person, trip, location):
    loc = location.copy().reset_index(drop=True)
    
    # calculate home distance
    _home = pd.merge(loc[['hh_id','trip_id']], 
                     hh[['hh_id','home_lat','home_lon']], how='left')
    _home = gpd.GeoDataFrame(data=_home,
                             geometry=_home.apply(lambda x: Point(x['home_lon'], x['home_lat']), axis=1), 
                             crs='epsg:4326').to_crs('epsg:2227')
    loc['dist_home'] = loc.distance(_home)
    
    # calculate work distances
    for p in range(1, person['person_num'].max()+1):
        print('distance to person {} work'.format(p))
        _work = pd.merge(loc[['hh_id','trip_id']],
                         person.loc[person['person_num'].eq(p),['hh_id','work_lat','work_lon']],
                         how='left')
        _work = gpd.GeoDataFrame(data=_work,
                                geometry=_work.apply(lambda x: Point(x['work_lon'], x['work_lat']), axis=1), 
                                crs='epsg:4326').to_crs('epsg:2227')
        loc['dist_work_{}'.format(p)] = loc.distance(_work)
    
    loc['dist_work'] = loc.apply(lambda x: x['dist_work_{}'.format(x['person_num'])], axis=1)
    
    # calculate school distances
    for p in range(1, person['person_num'].max()+1):
        print('distance to person {} school'.format(p))
        _school = pd.merge(loc[['hh_id','trip_id']],
                           person.loc[person['person_num'].eq(p),['hh_id','school_lat','school_lon']],
                           how='left')
        _school = gpd.GeoDataFrame(data=_school,
                                  geometry=_school.apply(lambda x: Point(x['school_lon'], x['school_lat']), axis=1), 
                                  crs='epsg:4326').to_crs('epsg:2227')
        loc['dist_school_{}'.format(p)] = loc.distance(_school)
    
    loc['dist_school'] = loc.apply(lambda x: x['dist_school_{}'.format(x['person_num'])], axis=1)
    
    # calculate second-home distances
    for p in range(1, person['person_num'].max()+1):
        print('distance to person {} second home'.format(p))
        _home = pd.merge(loc[['hh_id','trip_id']],
                         person.loc[person['person_num'].eq(p),['hh_id','second_home_lat','second_home_lon']],
                         how='left')
        _home = gpd.GeoDataFrame(data=_home,
                                geometry=_home.apply(lambda x: Point(x['second_home_lon'], x['second_home_lat']), axis=1), 
                                crs='epsg:4326').to_crs('epsg:2227')
        loc['dist_second_home_{}'.format(p)] = loc.distance(_home)
    
    loc['dist_second_home'] = loc.apply(lambda x: x['dist_second_home_{}'.format(x['person_num'])], axis=1)
    return loc

def split_trips(hh, person, day, trip, location, threshold=60):
    trip['trip_part'] = 0
    location['trip_part'] = 0
    
    to_split = location.loc[location['duration'].ge(threshold),'trip_id'].drop_duplicates().tolist()
    final_to_split = []
    
    loc = location.loc[location['trip_id'].isin(to_split)]
    loc = append_distances(hh, person, trip, loc)
    
    new_loc = []
    new_trips = []
    for trip_id in to_split:
        trip_part = 0
        
        _loc = loc.loc[loc['trip_id'].eq(trip_id)]
        _trip = trip.loc[trip['trip_id'].eq(trip_id)]
        
        if len(_loc) <=2:
            # can't split a trip if there are only 2 points
            continue
        
        final_to_split.append(trip_id)
        
        i = _loc.index[0]
        last_j = _loc.index[-1]
        for j, p in _loc.loc[i:].iterrows():
            if p['duration'] >= (threshold) or j==last_j:
                if i == j:
                    continue
                tmp = _loc.loc[i:j].copy()
                
                # calculate new start time using average speed
                if tmp.loc[i,'duration'] > threshold:
                    t = tmp.loc[i+1:, 'duration'].sum()
                    d = tmp.loc[i+1:, 'dist_next'].sum()

                    if d > 0 and t > 0:
                        s = d / t # feet per minute
                    elif len(tmp.loc[tmp['speed'].ge(0)]) > 0:
                        s = tmp.loc[tmp['speed'].ge(0),'speed'].mean() *(5280.0/60.0)
                    else:
                        s = 0
                        
                    tmp.loc[i, 'speed_next'] = s * (60.0 / 5280.0)
                    tmp.loc[i, 'duration'] = 0 if s == 0 else tmp.loc[i,'dist_next'] / s
                    tmp.loc[i, 'timestamp_local'] = tmp.loc[i+1,'timestamp_local'] - dt.timedelta(minutes=tmp.loc[i,'duration'])
                    #tmp.loc[i, 'dow'] = tmp.loc[i, 'timestamp_local'].weekday()
                    
                # update ending record
                tmp.loc[j,'duration'] = np.nan
                tmp['trip_part'] = trip_part
                
                if tmp['dist_next'].sum() > 0:
                    nt = get_trip(tmp, _trip, day)
                    
                    if isinstance(nt, pd.DataFrame):
                        nt['trip_part'] = trip_part
                        new_loc.append(tmp)
                        new_trips.append(nt)
                i = j
                trip_part += 1
    
    print('original trips: {}, trips after splitting: {}'.format(len(final_to_split), len(new_trips)))
    trip = trip.loc[~trip['trip_id'].isin(final_to_split)]
    trip = pd.concat([trip, pd.concat(new_trips)])
    trip = trip.sort_values(['hh_id','person_num','day_num','trip_id','trip_part']).reset_index(drop=True)
    
    # create new trip index/num
    trip['trip_id_sfcta'] = 0
    trip['trip_num_sfcta'] = 0
    i = 1
    person_id = -1
    
    for idx, row in trip.iterrows():
        if person_id != row['person_id']:
            i = 1
        person_id = row['person_id']
        trip.loc[idx, 'trip_num_sfcta'] = i
        trip.loc[idx, 'trip_id_sfcta'] = person_id * 100 + i
        i += 1
    
    location = location.loc[~location['trip_id'].isin(final_to_split)]
    location = pd.concat([location, pd.concat(new_loc)[location.columns.tolist()]])
    location = location.sort_values(['trip_id','trip_part','timestamp_local']).reset_index(drop=True)
    location = pd.merge(location, trip[['trip_id','trip_part','trip_num_sfcta','trip_id_sfcta']], how='left')
    return trip, location
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    preprocess(config)
