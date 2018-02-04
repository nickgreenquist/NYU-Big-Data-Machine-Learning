from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


vectorHash = {}

def mapper(x):
    csv = x.split(',')
    vectorValue = 0
    if csv[2] in vectorHash:
        vectorValue = int(vectorHash[csv[2]])
    return (csv[1], int(csv[3]) * vectorValue )


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit matrix.py <input matrix file> <input vector file> <output file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="MatrixMultiply")

    veclines = sc.textFile(sys.argv[2], 1)
    pairs = veclines.map(lambda x: (x.split(',')[1], x.split(',')[3]))
    vector = pairs.reduceByKey(lambda x,y: x + y).collect()
    for v in vector:
        vectorHash[v[0]] = v[1]

    lines = sc.textFile(sys.argv[1], 1)
    pairs = lines.map(mapper)
    ans = pairs.reduceByKey(lambda x,y: x + y)

    ans.saveAsTextFile(sys.argv[3])

        #words = lines.flatMap(lambda x: x.split(' '))
        #counts = words.map(lambda x: (x, 1))
        #wordcount = counts.reduceByKey(lambda x,y: x + y)

        #wordcount.saveAsTextFile(sys.argv[2])

    sc.stop()
