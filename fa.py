
from pyspark import SparkContext
import sys


sc = SparkContext()
TWEETS_100M = sys.argv[1]
tweets = sc.textFile(TWEETS_100M,use_unicode=True).cache()
ct_population = "500cities_tracts.geojson"

def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd

    cts = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(cts.plctract10):
	index.insert(idx,geometry.bounds)

    return (index,cts)


def findZone(p, index,cts):
    match = index.intersection((p.x,p.y,p.x,p.y))
    for idx in match:
        if cts.geometry[idx].contains(p):
            return idx
    return None
           



# extract the relevant components from the tweets csv file:
# which for now is the tweet id, tweet longitude and tweet latitude:


def extractAttributes(pid,rows):

    if pid==0:
	next(rows)
    import csv
    reader = csv.reader(rows,delimiter='|')

    # extract tweet id and longitude and latitude:
    for fields in reader:
	yield(fields[0],(float(fields[1]),float(fields[2])))

tweets_rdd = tweets.mapPartitionsWithIndex(extractAttributes)
tweets_deets =tweets_rdd.collect()
print(tweets_deets[0:5])



#

def processTweets(pid,records):
    import csv
    import pyproj
    import shapely.geometry as geom

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, cts = createIndex('500cities_tracts.geojson')
    
    reader = csv.reader(records)
    counts = {}
    for row in reader:
        tweet_location = geom.Point(proj(float(row[1]), float(row[2])))

        ct = findZone(tweet_location,index,cts)
        if ct:
           counts[ct] = counts.get(ct,0) + 1
    return counts.items()
