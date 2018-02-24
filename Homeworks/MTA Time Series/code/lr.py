from __future__ import print_function

import sys
import math
import operator

from pyspark import SparkContext

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

def mapper(line):
    val = [float(x) for x in line.split(",")]
    start = 1406880000
    if len(val) <= 1:
	return LabeledPoint(5, [0])
    if math.isnan(val[1]):
	return LabeledPoint(5, [0])
    return LabeledPoint(val[1], [(val[0] - 1406880000) / 600])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit lr.py <input timseries file> <output file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Time Series Linear Regression")

    lines = sc.textFile(sys.argv[1], 1)   

    pairs = lines.map(mapper)

    start = 1406880000
    train = pairs.filter(lambda x: x.features[0] < (1412063400 - start) / 600 )
    test = pairs.filter(lambda x: x.features[0] >= (1412063400 - start) / 600)

    model = LinearRegressionWithSGD.train(train, iterations=100, step=0.00000001)

    for i in range(50):
	print("MODEL")

    valuesAndPreds = test.map(lambda p: (p.label, model.predict(p.features)))
    t = valuesAndPreds.collect()
    for v in t:
	print(v)
    MSE = valuesAndPreds \
		.map(lambda vp: (vp[0] - vp[1])**2) \
		.reduce(lambda x, y: x + y) / valuesAndPreds.count()

    test = test.collect()

    print("Mean Squared Error = " + str(MSE))

    sc.stop()
