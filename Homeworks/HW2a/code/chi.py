from __future__ import print_function

import sys
import operator

from pyspark import SparkContext

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors

def mapper(x):
    return 0,abs( float(x) )

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit lr.py <input residuals file> <output file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Chi Squared residuals")

    lines = sc.textFile(sys.argv[1], 1)

    resid = lines.map(mapper)

    resid = resid.collect()

    residuals = []

    for r in resid:
	    residuals.append(r[1])

    vec = Vectors.dense(residuals)

    gft = Statistics.chiSqTest(vec)

    print("%s\n" % gft)

    sc.stop()
