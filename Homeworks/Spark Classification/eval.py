import sys

from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql import SQLContext

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: spark-submit eval.py")
        exit(-1)

    sc = SparkContext(appName="Classifier")
    sqlContext = SQLContext(sc)

    scoreAndLabels = []
    file = open("scoreAndLabels.txt", "r")
    for line in file:
	    pair = line.split(",")
        actual = float(pair[0])
	pred = float(pair[1])
	tup = (actual,pred)
	scoreAndLabels.append(tup)
    file.close()

    dataset = sqlContext.createDataFrame(scoreAndLabels, ["label", "prediction"])
    metrics = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="f1")
    f1 = metrics.evaluate(dataset)
    print("F1 Score: %s\n" % (f1))

    metrics = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedRecall")
    recall = metrics.evaluate(dataset)
    print("Recall: %s\n" % (recall))

    metrics = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="weightedPrecision")
    prec = metrics.evaluate(dataset)
    print("Precision: %s\n" % (prec))
     
    sc.stop()
