# File locations, contents, attributes
INPUT_FILE = r'..\input\locations_for_mm.csv'
OUTPUT_FILE = r'..\output\paths.csv'
PATH_ID = ['hh_id','person_id','trip_id'] # string or list
ORDER = ['hh_id','person_id','trip_id','collected_at'] # unused for now
POINT_ID = 'point_id'
X = 'lon'
Y = 'lat'
INFO_ATTRIBUTES = ['mode_type','accuracy','heading','speed','collected_at']

# MODE NOT IMPLEMENTED.
MODE = ['mode_type']
MODE_TO_NETTYPE = {1:'walk', # Walk
                   2:'bike', # Bike
                   3:'auto', # Car
                   4:'auto', # Taxi
                   5:'transit', # Transit
                   6:'auto', # Schoolbus
                   7:'all', # Other
                   8:'auto', # Shuttle/Vanpool
                   9:'auto', # TNC
                   10:'auto', # Carshare
                   11:'bike', # Bikeshare
                   12:'bike', # Scooter Share
                   13:'all', # Long Distance Passenger Mode
                   -9998:'all',
                   995:'all',
                   }

NETTYPE_TO_EDGE_CLASSES = {'walk': [201,204,203,202,120,121,118,119,111,117,114,106,107,110,100,108,124,112,115,122,109,125,113,104,105,123,401,301,302,303,304,305],
                           'bike': [201,204,203,202,120,121,118,111,117,114,106,107,110,100,108,124,112,115,109,125,113,104,105,123,401,301,302,303,304,305],
                           'auto': [201,204,203,202,121,111,101,103,102,106,107,110,100,108,124,112,115,109,125,104,105,123,401],
                           'transit': [201,204,203,202,116,121,111,101,103,102,106,107,110,100,108,124,112,115,109,125,104,105,123,401],
                           'all': None
                           }

# Run settings
CHUNKSIZE = 100000 
WRITE_CSV = True
WRITE_HDF = True
WRITE_FULL_PATH = True
NODES = ['123.45.6.789']
LOWER_BOUND, UPPER_BOUND = 3, 4 
CONNECTION_STRING = "host={} port={} dbname={} user={} password={}".format('localhost', 5432 , 'dbname', 'username', 'password')
DBTABLE = 'ways' 
SEARCH_RADIUS = 30
MAX_ROUTE_DISTANCE = 10000
