from __future__ import print_function

import sys
import re
import operator
from operator import add

from pyspark import SparkContext


iterations = 40
B = 0.8
count = 0

def mapper(x):
    if "#" not in x:
    	pair = re.split(r'\s+', x)
    	src = int(pair[0])
    	dest = int(pair[1])
    	return src, dest
    else:
	return -1,-1

def flatMapper(x):
    links = x[1][0]
    rank = x[1][1]
    n = len(links)
    value = rank / n
    for link in links:
	yield (link, value)

def reducer(x, y):
    return x + y

def initialRank(x):
    src = x[0]
    return (src, 1.0)

def damper(x):
    updatedRank = (x * B) + ((1 - B) / count)
    return updatedRank


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit pagerank.py <input graph file> <output file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Page Rank")

    lines = sc.textFile(sys.argv[1], 1)   

    linkPairs = lines.map(mapper).distinct().groupByKey()

    count = linkPairs.count()

    ranks = linkPairs.map(initialRank)

    for i in range(0, iterations):
	allPairs  = linkPairs.join(ranks).flatMap(flatMapper)
 
	ranks = allPairs.reduceByKey(lambda x,y: x + y)
	
	'''Use a damping factor to escape dead dends and spider traps'''
	#ranks = ranks.mapValues(damper)
	
    file = open(sys.argv[2], "w")
    for (link, rank) in ranks.sortBy(lambda x: -x[1]).collect():
	print("%s\t%s" % (link, rank))
	file.write("%s\t%s" % (link, rank))
	file.write("\n")
    file.close()
    #ranks.sortByKey().saveAsTextFile(sys.argv[2])	

    sc.stop()
