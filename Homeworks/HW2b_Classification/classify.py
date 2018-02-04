from __future__ import print_function
import sys
import re

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.ml.feature import Tokenizer, RegexTokenizer, HashingTF, IDF
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import NaiveBayes

from sklearn.datasets import fetch_20newsgroups

from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *


def load_data(train_or_test):
    data = fetch_20newsgroups(subset=train_or_test, shuffle=True, remove=('headers', 'footers', 'quotes'))
    return data.data, data.target


def preprocessor(data):
    #only want alpha characters
    data = re.sub("[^a-zA-Z]", " ", data)
    data = data.lower()
    
    #trim random chars floating around
    data = ' '.join([d for d in data.split() if len(d)>1])

    return data

def mapper(x):
   return (float(x[0]), x[1])

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usage: spark-submit classify.py")
        exit(-1)

    sc = SparkContext(appName="Classifier")
    sqlContext = SQLContext(sc)

    shape = StructType([
	StructField("label", DoubleType(), True),
	StructField("text", StringType(), True),
    ])

    train_text, train_labels = load_data('train')
    test_text, test_labels = load_data('test')

    #Have to zip the dataframe rdd with labels and text
    train = zip(train_labels, train_text)
    train = map(mapper, train)
    
    test = zip(test_labels, test_text)
    test = map(mapper, test)

     #set up Tokenizer
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")

    #set up preprocessor
    preprocessor_df = udf(preprocessor, StringType())

    #set up train dataframe tokenzied
    train_df = sqlContext.createDataFrame(train, shape)  
    train_df = train_df.withColumn('text', preprocessor_df(train_df.text))
    train_tokenized = tokenizer.transform(train_df)

    #set up test dataframe tokenized
    test_df = sqlContext.createDataFrame(test, shape)
    test_df = test_df.withColumn('text', preprocessor_df(test_df.text))
    test_tokenized = tokenizer.transform(test_df)

    #prepare rf and idf models
    htf = HashingTF(inputCol="tokens", outputCol="vectors")
    idf = IDF(inputCol="vectors", outputCol="features")

    #set up tf-idf vectors
    train_tf = htf.transform(train_tokenized)
    train_tf.cache()
    train_tf_idf = idf.fit(train_tf).transform(train_tf)

    test_tf = htf.transform(test_tokenized)
    test_tf.cache()
    test_tf_idf = idf.fit(test_tf).transform(test_tf)

    #set up NaiveBayes model
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", featuresCol="features", labelCol="label")
    model = nb.fit(train_tf_idf)
    result = model.transform(test_tf_idf)

    #gather results and save to file
    scoreAndLabels = result.map(lambda x: (x[0], x[7]))
    scoreAndLabels = scoreAndLabels.collect()
    file = open("naivebayes_results.txt", "w")
    for tup in scoreAndLabels:
        actual = tup[0]
	    pred = tup[1]
	    file.write("%s,%s\n" % (actual, pred))
    file.close()

    '''
    #NaiveBayes using Bag of Words - matrix multiplication error on Spark
    cv = CountVectorizer(inputCol="words", outputCol="vectors")
    train_cv = cv.fit(train_df_tokenized)
    train_cv = train_cv.transform(train_df_tokenized)

    test_cv = cv.fit(test_df_tokenized)
    test_cv = test_cv.transform(test_df_tokenized) 

    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", featuresCol="vectors", labelCol="label")
    model = nb.fit(train_cv)
    result = model.transform(test_cv)

    scoreAndLabels = result.map(lambda x: (x[0], x[6]))
    scoreAndLabels = scoreAndLabels.collect()
    print(scoreAndLabels)
    '''

    sc.stop()
