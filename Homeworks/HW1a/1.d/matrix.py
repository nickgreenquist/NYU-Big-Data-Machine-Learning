

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

vectorHash = {}

def mapper(x):
    csv = x.split(',')
    vectValue = 0
    if csv[2] in vectorHash:
         vectValue = int(vectorHash[csv[2]])
    return (csv[1], int(csv[3]) * vectValue )


if __name__ == "__main__":
        if len(sys.argv) != 12:
                print("Usage: spark-submit matrix.py 5<input matrix files> 5<input vector files> <output file>", file=sys.stderr)
                exit(-1)

        sc = SparkContext(appName="MatrixMultiply")

        veclines = sc.textFile(sys.argv[6], 1)
        pairs = veclines.map(lambda x: (x.split(',')[1], x.split(',')[3]))
        vector = pairs.reduceByKey(lambda x,y: x + y).collect()
        for v in vector:
                vectorHash[v[0]] = v[1]
        m_lines = sc.textFile(sys.argv[1], 1)
        m_pairs = m_lines.map(mapper)
        m_total = m_pairs.reduceByKey(lambda x,y: x+y)

        for i in range(1, 5):
                vectorHash.clear()
                veclines = sc.textFile(sys.argv[6 + i], 1)
                pairs = veclines.map(lambda x: (x.split(',')[1], x.split(',')[3]))
                vector = pairs.reduceByKey(lambda x,y: x + y).collect()
                for v in vector:
                        vectorHash[v[0]] = v[1]

                m_lines = sc.textFile(sys.argv[1 + i], 1)
                m_pairs = m_lines.map(mapper)
                m_total = m_total.union(m_pairs).reduceByKey(lambda x,y: x+y)

        m_total.saveAsTextFile(sys.argv[11])

        sc.stop()
