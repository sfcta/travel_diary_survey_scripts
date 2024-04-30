# map_matcher README
`Usage: python map_matcher.py control.py`

`control.py` should be modified by the user to specify input parameters

## I/O
- INPUT_FILE: _str_ the input filepath.  The input file should be a csv file where each record is a point location.  The file should be sorted such that all points for a path are together, and the points within each path are in order.  If not, you can specify the columns to sort by using `ORDER`
- OUTPUT_FILE: _str_ the output filepath.
- PATH_ID: _str_ or _list_.  A column name, or list of column names, that uniquely identifies a path.  A path is an ordered collection of points.
- ORDER: _str_ or _list_.  A column name, or list of column names, that specifies the sort order.  
- POINT_ID: _str_ The column name of a field that uniquely identifies a record.
- X: _str_ the column name of the field containing the X coordinate.
- Y: _str_ the column name of the field containing the Y coordinate. 
- INFO_ATTRIBUTES: _list_ A list of column names in the `INPUT_FILE` that should be carried over to the `OUTPUT_FILE`

## routing settings
- MODE: _str_.  **This _might_ not work yet.**  The column name of the field specifying the mode. This column should contain values that match the keys in `MODE_TO_NETTYPE`
- MODE_TO_NETTYPE: _dict_.  A mapping of mode values from the `INPUT_FILE` to mode types.  The mode types are keys in `NETTYPE_TO_EDGE_CLASSES`.
- NETTYPE_TO_EDGE_CLASSES: _dict_.  A mapping of mode type to OSM class_ids.  This is used to define which facility types are available for routing.  For example the 'auto' mode type would not contain any pedestrian-only class_ids.  


## run settings
- CHUNKSIZE: _int64_.  The number of records that should be processed at a time, or the number of records that should be dispatched in a single job.
- WRITE_CSV: _bool_.  If `True`, write output as a .csv file
- WRITE_HDF: _bool_.  If `True`, write output as an .h5 file
- NODES: _list_.  List of IP addresses of worker machines for distributed processing.
- LOWER_BOUND: _int_.  When a node is processing fewer than this number of jobs, check out a new one.
- UPPER_BOUND: _int_.  If this many jobs are checked out, do not check out a new one.  
- CONNECTION_STRING: _str_.  Login credentials for a postgis database with pgrouting
- DBTABLE: _str_.  The name of the table containing routable OSM links.  
- SEARCH_RADIUS: _int_.  In meters, a buffer distance to select links from `DBTABLE` to be included in routing
- MAX_ROUTE_DISTANCE: _int_.  In meters, the maximum distance to build a path between two nodes before giving up.  
- WRITE_FULL_PATH: _bool_.  If True, write both the links associated with a point in `INPUT_FILE` AND write links for the path connecting those points.  If `False` only write the links associated with points from `INPUT_FILE`. 
