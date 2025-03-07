import argparse
import tomllib
from pathlib import Path
from shutil import copy2
import pandas as pd

def hhmm_to_minutes(t):
    if pd.isnull(t):
        return t
    h = int(t / 100)
    m = (t - h*100)
    return (h*60) + m

def impute_missing_work_locations(config):
    indir = Path(config['04b-adjust_weights']['dir'])
    outdir = Path(config['04c-impute_missing_work_locations']['dir'])
    outdir.mkdir(exist_ok=True, parents=True)

    person = pd.read_csv(indir / config['person_filename'])
    tour = pd.read_csv(indir / config['tour_filename'])
    
    
    tour['duration'] = tour['tlvdest'].map(lambda x: hhmm_to_minutes(x)) - tour['tardest'].map(lambda x: hhmm_to_minutes(x))

    wtd = (tour.loc[tour['pdpurp'].eq(1)]
               .groupby(['hhno','pno','tdtaz','tdpcl'])
               .agg({'duration':['min','max','mean','sum'],
                     'pno':'count',
                     'tdxco':'mean',
                     'tdyco':'mean'}))
    wtd.columns = ['min','max','mean','sum','count','tdxco','tdyco']
    
    # work tours with an activity time at least 4 hours
    work_loc = wtd.loc[wtd['max'].ge(240)].reset_index()[['hhno','pno','tdtaz','tdpcl','tdxco','tdyco','mean','sum']]
    
    # for each person, keep the tour dest with the greatest total activity time
    work_loc = work_loc.loc[work_loc.groupby(['hhno','pno'])['sum'].idxmax()]
    
    work_loc.reset_index(drop=True)
    work_loc['tdtaz'] = work_loc['tdtaz'].fillna(-1)
    
    person = pd.merge(person, 
                      work_loc[['hhno','pno','tdtaz','tdpcl','tdxco','tdyco']],
                      how='left')
    person[['tdtaz','tdpcl','tdxco','tdyco']] = person[['tdtaz','tdpcl','tdxco','tdyco']].fillna(-1)
    person.loc[person['pwtaz'].eq(-1) & person['tdtaz'].ne(-1), 'pwpcl'] = person['tdpcl']
    person.loc[person['pwtaz'].eq(-1) & person['tdtaz'].ne(-1), 'pwxco'] = person['tdxco']
    person.loc[person['pwtaz'].eq(-1) & person['tdtaz'].ne(-1), 'pwyco'] = person['tdyco']
    person.loc[person['pwtaz'].eq(-1) & person['tdtaz'].ne(-1), 'pwtaz'] = person['tdtaz']
    
    person.drop(columns=['tdtaz','tdpcl','tdxco','tdyco'], inplace=True)
    person.to_csv(outdir / config['person_filename'], index=False)
    
    day_filename = 'personday.csv'
    hh_filename = config["hh_filename"]
    vehicle_filename = config["vehicle_filename"]
    tour_filename = config['tour_filename']
    trip_filename = config['trip_filename']
    
    copy2(indir / day_filename, outdir / day_filename)
    copy2(indir / hh_filename, outdir / hh_filename)
    #copy2(indir / vehicle_filename, outdir / vehicle_filename)
    copy2(indir / tour_filename, outdir / tour_filename)
    copy2(indir / trip_filename, outdir / trip_filename)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_filepath')
    args = parser.parse_args()
    with open(args.config_filepath, 'rb') as f:
        config = tomllib.load(f)
    impute_missing_work_locations(config)
