__doc__ = '''
usage: 

OPTION 1:
  python map_matcher.py PSQL_URI ROAD_TABLE_NAME [SEARCH_RADIUS = 30] [MAX_ROUTE_DISTANCE = 2000] < sequence.txt

  The PSQL_URI looks like:
      "host=localhost port=5432 dbname=road_network user=postgres password=secret"

  ROAD_TABLE_NAME is the table imported by osm2pgrouting.

  SEARCH_RADIUS is a range value in meters within which the program will
  search for each measurement's candidates. Think of it as GPS accuracy.

  MAX_ROUTE_DISTANCE is the maximum allowed route distance (in meters)
  between two concussive measurements.

OPTION 2:
  python map_matcher.py control.py
  
  Where the following attributes are defined in control.py:
    INPUT_FILE, OUTPUT_FILE
    PATH_ID, ORDER, POINT_ID, X, Y, INFO_ATTRIBUTES,
    MODE, MODE_TO_NETTYPE, NETTYPE_TO_EDGE_CLASSES,
    CHUNKSIZE
    WRITE_CSV, WRITE_HDF
    NODES, LOWER_BOUND, UPPER_BOUND
    CONNECTION_STRONG, DBTABLE
    SEARCH_RADIUS
    MAX_ROUTE_DISTANCE
'''


import sys, os, dispy, threading, getopt, logging
import psycopg2
import cPickle as pickle
import pandas as pd
import numpy as np
import geopandas as gpd
from datetime import datetime
import datetime as dt
from shapely.geometry import Point

# NOTE: If map_matching is installed somewhere other than the default (i.e. Lib\site-packages), then need to add the path
# here AND in the functions that are distributed through dispy
#sys.path.insert(0,<MAPMATCHINGPATH>)  
import map_matching as mm
from map_matching.utils import Edge, Measurement

# File locations, contents, attributes
INPUT_FILE = '<input file path>'
OUTPUT_FILE = '<output file path>'
PATH_ID = '<key>' # string or list
ORDER = ['<order by 1>','<order1 by 2>'] # unused for now
POINT_ID = '<point_id>' # string or list
X = '<x>'
Y = '<y>'
INFO_ATTRIBUTES = ['<extra>','<field>','<names>','<to>','<include>']

MODE = ['<mode_field_name>']
MODE_TO_NETTYPE = {'<mode_1>':'<mode_type_1>', 
                   '<mode_2>':'<mode_type_2>',
                   }
NETTYPE_TO_EDGE_CLASSES = {'mode_type_1': ['<class_id_1>','<class_id_2>'],
                           }
                   
# Run settings
CHUNKSIZE = 600000 
WRITE_CSV = True
WRITE_HDF = True
NODES = ['###.##.#.###']  # IP Address of the 
LOWER_BOUND, UPPER_BOUND = 36, 38 
CONNECTION_STRING = "host={} port={} dbname={} user={} password={}".format('localhost', 5432 , 'dbname', 'username', 'password')
DBTABLE = 'ways' 
SEARCH_RADIUS = 30
MAX_ROUTE_DISTANCE = 2000
#BIDIRECTIONAL_NET = False # TODO
WRITE_FULL_PATH = True

def generate_placeholder(length, width):
    """
    Generate "(%s, %s, %s, ...), ..." for placing parameters.
    """
    return ','.join('(' + ','.join(['%s'] * width) + ')' for _ in range(length))

def create_sequence_subquery(length, columns):
    """Create a subquery for sequence."""
    placeholder = generate_placeholder(length, len(columns))
    subquery = 'WITH sequence {columns} AS (VALUES {placeholder})'.format(
        columns='(' + ','.join(columns) + ')',
        placeholder=placeholder)
    return subquery


def query_edges_in_sequence_bbox(conn, road_table_name, sequence, search_radius, edge_classes=None):
    #sys.path.insert(0,<MAPMATCHINGPATH>)  
    import map_matching as mm
    from map_matching.utils import Edge, Measurement
    """
    Query all road edges within the bounding box of the sequence
    expanded by search_radius.
    """
    if not sequence:
        return

    subquery = create_sequence_subquery(len(sequence), ('lon', 'lat'))

    stmt = subquery + '''
    -- NOTE the cost unit is in km
    SELECT edge.gid, edge.source, edge.target, edge.length_m * edge.cost / abs(edge.cost), edge.length_m * edge.reverse_cost / abs(edge.reverse_cost)
    FROM {road_table_name} AS edge
         CROSS JOIN (SELECT ST_Extent(ST_MakePoint(sequence.lon, sequence.lat))::geometry AS extent FROM sequence) AS extent
    WHERE edge.the_geom && ST_Envelope(ST_Buffer(extent.extent::geography, %s)::geometry)
        AND edge.cost != 0
        AND edge.reverse_cost != 0
    '''.format(road_table_name=road_table_name)

    if isinstance(edge_classes, list) or isinstance(edge_classes, tuple):
        stmt = stmt + '''
            AND edge.class_id in {edge_classes}
        '''.format(edge_classes=tuple(edge_classes))

    # Aggregate and flatten params
    if len(sequence[0]) == 3:
        params = sum([[lon, lat] for idx, lon, lat in sequence], [])
    else:
        params = sum([[lon, lat] for lon, lat in sequence], [])
    params.append(search_radius)

    cur = conn.cursor()
    cur.execute(stmt, params)

    for gid, source, target, cost, reverse_cost in cur.fetchall():
        edge = Edge(id=gid,
                    start_node=source,
                    end_node=target,
                    cost=cost,
                    reverse_cost=reverse_cost)
        yield edge

    cur.close()


def build_road_network(edges, allow_bidirectional=False):
    """Construct the directional road graph given a list of edges."""
    graph = {}

    # Graph with bidirectional edges
    if allow_bidirectional:
        for edge in edges:
            graph.setdefault(edge.start_node, []).append(edge)
            graph.setdefault(edge.end_node, []).append(edge.reversed_edge())
        
    # Graph with directional edges
    else:
    
        for edge in edges:
            if edge.cost > 0:
                graph.setdefault(edge.start_node, []).append(edge)
            else:
                graph.setdefault(edge.start_node, [])
            if edge.reverse_cost > 0:
                graph.setdefault(edge.end_node, []).append(edge.reversed_edge())
            else:
                graph.setdefault(edge.end_node, [])
    return graph

def query_candidates(conn, road_table_name, sequence, search_radius, edge_classes=None):
    #sys.path.insert(0,<MAPMATCHINGPATH>)  
    import map_matching as mm
    from map_matching.utils import Edge, Measurement
    from Candidate import Candidate
    """
    Query candidates of each measurement in a sequence within
    search_radius.
    """
    subquery = create_sequence_subquery(len(sequence), ('id', 'lon', 'lat'))

    subquery = subquery + ',' + '''
    --- WITH sequence AS (subquery here),
    seq AS (SELECT *,
                   ST_SetSRID(ST_MakePoint(sequence.lon, sequence.lat), 4326) AS geom,
                   ST_SetSRID(ST_MakePoint(sequence.lon, sequence.lat), 4326)::geography AS geog
        FROM sequence)
    '''

    stmt = subquery + '''
    SELECT seq.id, seq.lon, seq.lat,
           --- Edge information
           edge.gid, edge.source, edge.target,
           edge.length_m * edge.cost / abs(edge.cost), edge.length_m * edge.reverse_cost / abs(edge.reverse_cost),

           --- Location, a float between 0 and 1 representing the location of the closest point on the edge to the measurement.
           ST_LineLocatePoint(edge.the_geom, seq.geom) AS location,

           --- Distance in meters from the measurement to its candidate's location
           ST_Distance(seq.geog, edge.the_geom::geography) AS distance,

           --- Candidate's location (a position along the edge)
           ST_X(ST_ClosestPoint(edge.the_geom, seq.geom)) AS clon,
           ST_Y(ST_ClosestPoint(edge.the_geom, seq.geom)) AS clat

    FROM seq CROSS JOIN {road_table_name} AS edge
    WHERE edge.the_geom && ST_Envelope(ST_Buffer(seq.geog, %s)::geometry)
          AND ST_DWithin(seq.geog, edge.the_geom::geography, %s)
          AND edge.cost != 0
          AND edge.reverse_cost != 0
    '''.format(road_table_name=road_table_name)
    
    if isinstance(edge_classes, list) or isinstance(edge_classes, tuple):
        stmt = stmt + '''
            AND edge.class_id in {edge_classes}
        '''.format(edge_classes=tuple(edge_classes))

    # Aggregate and flatten params
    if len(sequence[0]) == 3:
        params = sum([[int(idx), lon, lat] for idx, lon, lat in sequence], [])
    else:
        params = sum([[idx, lon, lat] for idx, (lon, lat) in enumerate(sequence)], [])
    
    params.append(search_radius)
    params.append(search_radius)

    cur = conn.cursor()
    cur.execute(stmt, params)

    for mid, mlon, mlat, \
        eid, source, target, cost, reverse_cost, \
        location, distance, \
        clon, clat in cur:

        measurement = Measurement(id=mid, lon=mlon, lat=mlat)

        edge = Edge(id=eid, start_node=source, end_node=target, cost=cost, reverse_cost=reverse_cost)
        
        assert 0 <= location <= 1
        
        candidate = Candidate(measurement=measurement, edge=edge, location=location, distance=distance)

        # Coordinate along the edge (not needed by MM but might be
        # useful info to users)
        candidate.lon = clon
        candidate.lat = clat
        
        # yield candidate

        if edge.cost > 0:
            yield candidate
        
        if edge.reverse_cost > 0:
            candidate = Candidate(measurement=measurement, edge=edge.reversed_edge(), location=(1-location), distance=distance)
            candidate.lon = clon
            candidate.lat = clat
            yield candidate

    cur.close()


def map_match(conn, road_table_name, sequence, search_radius, max_route_distance, edge_classes=None, allow_bidirectional=False):
    #sys.path.insert(0,<MAPMATCHINGPATH>)  
    import map_matching as mm
    from map_matching.utils import Edge, Measurement
    """Match the sequence and return a list of candidates."""

    # Prepare the network graph and the candidates along the sequence
    edges = query_edges_in_sequence_bbox(conn, road_table_name, sequence, search_radius, edge_classes)
    network = build_road_network(edges, allow_bidirectional=allow_bidirectional)
    candidates = query_candidates(conn, road_table_name, sequence, search_radius, edge_classes)

    # If the route distance between two consive measurements are
    # longer than `max_route_distance` in meters, consider it as a
    # breakage
    matcher = mm.MapMatching(network.get, max_route_distance)
    
    # Match and return the selected candidates along the path
    return list(matcher.offline_match(candidates))


def parse_argv(opts, args):
    for o, a in opts:
        if o=='-d':
            debug_mode = True
        if o=='-h':
            hwy_mode = True
    args = args + [None, None]

    return 

def job_callback(job): # executed at the client
    global submit_queue, jobs_cond, lower_bound
    if (job.status == dispy.DispyJob.Finished  # most usual case
        or job.status in (dispy.DispyJob.Terminated, dispy.DispyJob.Cancelled,
                          dispy.DispyJob.Abandoned)):
        # 'pending_jobs' is shared between two threads, so access it with
        # 'jobs_cond' (see below)
        jobs_cond.acquire()
        if job.id: # job may have finished before 'main' assigned id
            submit_queue.pop(job.id)
            dispy.logger.info('job "%s" done with %s: %s', job.id, job.result, len(submit_queue))
            if len(submit_queue) <= lower_bound:
                jobs_cond.notify()
        else:
            dispy.logger.info('no job.id')
        jobs_cond.release()
        
def proc(uri, road_table_name, ifile, search_radius, max_route_distance, ofile, 
         path_id='trip_id', order_by=None, point_id='id', x='lon', y='lat', info_attr=[], 
         mode_type='mode_type', mode_to_net=None, net_to_classes=None, write_full_path=True):
    #sys.path.insert(0,<MAPMATCHINGPATH>)  
    import map_matching as mm
    from map_matching.utils import Edge, Measurement
    import dispy, psycopg2
    import cPickle as pickle
    import pandas as pd
    import numpy as np
    import geopandas as gpd
    import datetime as dt
    from shapely.geometry import Point
    
    f = file(ifile,'rb')
    gps = pickle.load(f)
    f.close()
    if not isinstance(gps, gpd.GeoDataFrame):
        print "converting to geodataframe"
        gps['geometry'] = gps.apply(lambda xx: Point((xx[x],xx[y])), axis=1)
        gps = gpd.GeoDataFrame(geometry=gps['geometry'], data=gps)
    if not gps.crs:
        gps.crs = {'init':"epsg:4326"}
    else:
        print "reprojecting to epsg:4326"
        gps = gps.to_crs({'init':"epsg:4326"})

    if path_id in gps.columns.tolist():
        print "found {} in columns".format(path_id)
        path_data = pd.DataFrame(gps)
    else:
        raise Exception('{} not found in columns'.format(path_id))
        
    if order_by != None:
        if isinstance(order_by, str):
            order_by = [order_by]
        for ob in order_by:
            if ob in gps.columns.tolist():
                print "found {} in columns".format(ob)
            else:
                raise Exception('{} not found in columns'.format(ob))
                
    path_data = path_data.sort_values(by=order_by)
    conn = psycopg2.connect(uri)
    md = []
    i = 0
    grouped = path_data.groupby(path_id)
    for pid, path in grouped:
        sequence = [[row[point_id], row[x],row[y]] for (idx, row) in path.iterrows()]
        extras = {row[point_id]:{key:row[key] for key in info_attr} for (idx, row) in path.iterrows()}
        if mode_to_net == None or net_to_classes == None:
            edge_classes = None
        else:
            #print path[mode_type].values[0][0]
            nettype = mode_to_net[path[mode_type].values[0][0]]
            allow_bidirectional = True if nettype in ['walk','bike'] else False
            edge_classes = net_to_classes[nettype]
        i += 1
        #print "%d of %d" % (i, len(grouped))
        try:
            candidates = map_match(conn, road_table_name, sequence, search_radius, max_route_distance, edge_classes, allow_bidirectional)
        except Exception as e:
            print e
            continue
        last_candidate = None
        for candidate in candidates:
            if write_full_path:
                if last_candidate:
                    if last_candidate in candidate.path.keys():
                        p = candidate.path[last_candidate]
                        for edge in reversed(p):
                            if (isinstance(edge.start_node, int) or isinstance(edge.start_node, long)) and (isinstance(edge.end_node, int) or isinstance(edge.end_node, long)):
                                data = [int(candidate.measurement.id),int(pid)]
                                data += [np.nan,np.nan]
                                data += [extras[candidate.measurement.id][key] for key in info_attr]
                                data += [int(edge.id),
                                         int(edge.start_node),
                                         int(edge.end_node),
                                         int(edge.reversed),
                                         np.nan,np.nan,
                                         np.nan,np.nan]
                                md.append(data)
            # now get the candidate
            data = [int(candidate.measurement.id),int(pid)]
            data += [float(candidate.lat),float(candidate.lon)]
            data += [extras[candidate.measurement.id][key] for key in info_attr]
            data += [int(candidate.edge.id),
                     int(candidate.edge.start_node),
                     int(candidate.edge.end_node),
                     int(candidate.edge.reversed),
                     float(candidate.measurement.lat),float(candidate.measurement.lon),
                     float(candidate.location),float(candidate.distance)]
            md.append(data)
            last_candidate = candidate
    #raise Exception("making matches dataframe\n%s" % str(md))
    matches = pd.DataFrame(data=md, columns=[point_id,path_id,'m_lat','m_lon']+info_attr+ \
              ['edge','source','target','reversed','c_lat','c_lon','loc','meters'])
    #print "opening tempfile %s" % ofile
    f = file(ofile,'wb')
    #print "dumping dataframe"
    pickle.dump(matches,f,pickle.HIGHEST_PROTOCOL)
    f.close()
    conn.close()
    return ofile

if __name__ == '__main__':
    import map_matching as mm
    #opts, args = getopt.getopt(sys.argv[1:], 'b:n:r:chpdh')
    args = sys.argv[1:]
    
    if len(args) == 1:
      execfile(args[0])
      debug_mode = False
    else:
      sys.exit(1)
      
    global lower_bound
    lower_bound, upper_bound = LOWER_BOUND, UPPER_BOUND
    now = dt.datetime.now()
    path_id_xwalk = None

    n, ext = os.path.splitext(INPUT_FILE)
    if ext == '.h5':
        reader = pd.read_hdf(INPUT_FILE,'data',chunksize=CHUNKSIZE)
    elif ext == '.csv':
        reader = pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE)
    elif ext == '.tsv':
        reader = pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE, sep='\t')
    else:
        sys.exit(1)
    last_trip_id = 0
    last_gps = None
    submit_queue, wait_queue = {}, {}
    to_merge = []
    jobs_cond = threading.Condition()
    cluster = dispy.JobCluster(proc, 
                               callback=job_callback,
                               nodes=NODES, 
                               depends=[query_edges_in_sequence_bbox,
                                        build_road_network,
                                        create_sequence_subquery,
                                        generate_placeholder,
                                        query_candidates,
                                        map_match],
                               loglevel=logging.info)
    chunk_number, total_records = 0, 0
    
    if isinstance(PATH_ID, list):
      path_id = 'index'
      INFO_ATTRIBUTES = INFO_ATTRIBUTES + PATH_ID
    else:
      path_id = PATH_ID
    
    for gps in reader:
        if isinstance(PATH_ID, list):
          if not isinstance(path_id_xwalk, pd.DataFrame):
            # get a zero-based column called 'index'
            df = pd.DataFrame(gps[PATH_ID]).drop_duplicates().set_index(PATH_ID).reset_index().reset_index()
          else:
            df2 = pd.DataFrame(gps[PATH_ID]).drop_duplicates().set_index(PATH_ID).reset_index().reset_index()
            df2['index'] = df2['index'] + df['index'].max() + 1
            df = df.append(df2)
            df.drop_duplicates(subset=PATH_ID, inplace=True)

        then, now = now, dt.datetime.now()
        chunk_number += 1
        total_records += len(gps)
        
        gps.set_index(PATH_ID, inplace=True)
        gps['index'] = df.set_index(PATH_ID)['index']
        gps.reset_index(inplace=True)
        
        print '%s: reading chunk %d with %d records, total records: %d in %0.2f' % (now, chunk_number, len(gps), total_records, (now-then).total_seconds())
        ifile = 'mm_input_%06d.dat' % chunk_number
        ofile = 'mm_output_%06d.dat' % chunk_number
        ifile = os.path.join(r'C:\Temp',ifile)
        ofile = os.path.join(r'C:\Temp',ofile)
        f = file(ifile,'wb')
        
        pickle.dump(gps,f,pickle.HIGHEST_PROTOCOL)
        f.close()
        if debug_mode:
             result = proc(CONNECTION_STRING, DBTABLE, ifile, SEARCH_RADIUS, MAX_ROUTE_DISTANCE, ofile,
                           path_id=path_id, order_by=ORDER, point_id=POINT_ID, x=X, y=Y, info_attr=INFO_ATTRIBUTES,
                           mode_type=MODE, mode_to_net=MODE_TO_NETTYPE, net_to_classes=NETTYPE_TO_EDGE_CLASSES,
                           write_full_path=WRITE_FULL_PATH)
             print result
             to_merge.append(result)
             continue
        job = cluster.submit(CONNECTION_STRING, DBTABLE, ifile, SEARCH_RADIUS, MAX_ROUTE_DISTANCE, ofile,
                             path_id=path_id, order_by=ORDER, point_id=POINT_ID, x=X, y=Y, info_attr=INFO_ATTRIBUTES,
                             mode_type=MODE, mode_to_net=MODE_TO_NETTYPE, net_to_classes=NETTYPE_TO_EDGE_CLASSES,
                             write_full_path=WRITE_FULL_PATH)
        jobs_cond.acquire()
        job.id = chunk_number
        if job.status == dispy.DispyJob.Created or job.status == dispy.DispyJob.Running:
            submit_queue[chunk_number] = job
            # wait for queue to fall before lower bound before submitting another job
            if len(submit_queue) >= upper_bound:
                while len(submit_queue) > lower_bound:
                    jobs_cond.wait()
        jobs_cond.release()
        
        wait_queue[chunk_number] = job
        pop_ids = []
        for jobid, job in wait_queue.iteritems():
            if job.status == dispy.DispyJob.Finished:
                tempfile = job.result
                print 'TEMPFILE:', tempfile
                pop_ids.append(jobid)
                to_merge.append(tempfile)
                #if tempfile == None:
                #    raise Exception('jobid: {} returned no tempfile'.format(jobid))
            elif job.status in [dispy.DispyJob.Abandoned, dispy.DispyJob.Cancelled,
                                dispy.DispyJob.Terminated]:
                print '-------------------------------'
                print '- jobid: %5d                 -' % job.id
                print '- %s' % job.status
                print '- %s' % job.stderr
                print '- %s' % job.stdout
                print '- %s' % job.exception
                print '-------------------------------'
        for jobid in pop_ids:
            wait_queue.pop(jobid)
    pop_ids = []
    for jobid, job in wait_queue.iteritems():
        try:
            tempfile = job()
            pop_ids.append(jobid)
            to_merge.append(tempfile)
            if tempfile == None:
                raise Exception('jobid: {} returned no tempfile'.format(jobid))
        except Exception as e:
            print e
            print '-------------------------------'
            print '- jobid: %5d                 -' % job.id
            print '- %s' % job.status
            print '- %s' % job.stderr
            print '- %s' % job.stdout
            print '- %s' % job.exception
            print '-------------------------------'
    for jobid in pop_ids:
        wait_queue.pop(jobid)
       
    OUTPUT_FILE, ext = os.path.splitext(OUTPUT_FILE)
    XWALK_PATH = os.path.split(OUTPUT_FILE)[0]
    for merge_file in to_merge:

        if merge_file is None:
            continue
            
        f = file(merge_file,'rb')
        matches = pickle.load(f)
        if WRITE_CSV:
            try:
                matches.to_csv(OUTPUT_FILE+'.csv',mode='a', index=False)
            except:
                "failed to write matches to csv (%s)" % merge_file
            try:
                matches.to_hdf(OUTPUT_FILE+'.h5', 'data',format='t', mode='a', append=True, index=False)
            except:
                "failed to write matches to hdf (%s)" % merge_file
        f.close()
        os.remove(merge_file)
    
    df.to_csv(os.path.join(XWALK_PATH,'xwalk_file.csv'))
    
#        for candidate in candidates:
#            print '         Measurement ID: {0}'.format(candidate.measurement.id)
#            print '             Coordinate: {0:.6f} {1:.6f}'.format(*map(float, (candidate.measurement.lon, candidate.measurement.lat)))
#            print '    Matche d coordinate: {0:.6f} {1:.6f}'.format(*map(float, (candidate.lon, candidate.lat)))
#            print '        Matched edge ID: {0}'.format(candidate.edge.id)
#            print 'Location along the edge: {0:.2f}'.format(candidate.location)
#            print '               Distance: {0:.2f} meters'.format(candidate.distance)
#            print
    
    then, now = now, dt.datetime.now()
    print "%s: done." % (now)
    

    