from __future__ import print_function
import sys
import operator
from operator import add
from pyspark import SparkContext
import re
import string
import binascii
import random
import math
from math import sqrt

'''from nltk.corpus import stopwords'''
STOP_WORDS = set([u'all', u'just', u'being', u'over', u'both', u'through', u'yourselves', u'its',\
                 u'before', u'o', u'hadn', u'herself', u'll', u'had', u'should', u'to', u'only', \
                 u'won', u'under', u'ours', u'has', u'do', u'them', u'his', u'very', u'they', u'not',\
                 u'during', u'now', u'him', u'nor', u'd', u'did', u'didn', u'this', u'she', u'each',\
                 u'further', u'where', u'few', u'because', u'doing', u'some', u'hasn', u'are', u'our',\
                 u'ourselves', u'out', u'what', u'for', u'while', u're', u'does', u'above', u'between',\
                 u'mustn', u't', u'be', u'we', u'who', u'were', u'here', u'shouldn', u'hers', u'by', u'on',\
                 u'about', u'couldn', u'of', u'against', u's', u'isn', u'or', u'own', u'into', u'yourself',\
                 u'down', u'mightn', u'wasn', u'your', u'from', u'her', u'their', u'aren', u'there', u'been',\
                 u'whom', u'too', u'wouldn', u'themselves', u'weren', u'was', u'until', u'more', u'himself',\
                 u'that', u'but', u'don', u'with', u'than', u'those', u'he', u'me', u'myself', u'ma', u'these',\
                 u'up', u'will', u'below', u'ain', u'can', u'theirs', u'my', u'and', u've', u'then', u'is', u'am',\
                 u'it', u'doesn', u'an', u'as', u'itself', u'at', u'have', u'in', u'any', u'if', u'again', u'no',\
                 u'when', u'same', u'how', u'other', u'which', u'you', u'shan', u'needn', u'haven', u'after',\
                 u'most', u'such', u'why', u'a', u'off', u'i', u'm', u'yours', u'so', u'y', u'the', u'having', u'once'])

maxShingleID = 2**32-1
nextPrime = 4294967311
numHashes = 100
numIterations = 30
    
def shingler(x):
    pair = x.split("\",")
    if len(pair) > 1:
    	article = pair[0]
        info = pair[1].split(",")
    	if len(info) > 1:
		heading = info[1]
		article = article.lower()
		article = "".join(l for l in article if l not in string.punctuation)
		article = article = article.encode('ascii', 'ignore').decode('ascii')
		article = re.sub("[^a-zA-Z]", " ", article)	
		shingles = article.split()
		shingles = list(set(shingles))
		shingles = [s for s in shingles if not s in STOP_WORDS]
    		return heading, shingles
	else:
	       return -1,[]
    else:
	return -1, []

def hashShingles(kvp):
	heading = kvp[0]
	shingles = kvp[1]	 
	hashedShingles = []
        for s in shingles:
        	#hs = binascii.crc32(s) & 0xffffffff
                hs = hash(s)
		hashedShingles.append(hs)
        return heading, hashedShingles

def pickRandomCoeffs(k):
  randList = []
  
  while k > 0:
    randIndex = random.randint(0, maxShingleID) 
    while randIndex in randList:
      randIndex = random.randint(0, maxShingleID) 
    
    randList.append(randIndex)
    k = k - 1
    
  return randList

def getSig(kvp):
    signature = []
    shingleIDSet = kvp[1]
    for i in range (0, numHashes):
	minHashCode = nextPrime + 1
	for shingleID in shingleIDSet:
	    hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime
	    hashCode = float(hashCode) / float(nextPrime)
	    if hashCode < minHashCode:
		minHashCode = hashCode

	signature.append(minHashCode)
    return kvp[0], signature 

def dist(x, center):
    sum_sq = 0.0
    for i,val in enumerate(center):
	diff = x[i] - center[i]
	sq = math.pow(diff,2)
	sum_sq += sq
    distance = math.sqrt(sum_sq)
    return distance

def calcmean(clusterMembers):
    c = [0.0] * numHashes
    n = 0
    for sig in clusterMembers:
	i = 0
	for v in sig[1]:
	    c[i] += v
	    i += 1
	n += 1
    for i in range(0, numHashes):
	if n < 1:
	    c[i] = c[i]
	else:
	    c[i] /= n
    return c

def distMapper(x, centers):
    heading = x[0]
    vec = list(x[1])
    curd = float('inf')
    center_index = -1
    for i,center in enumerate(centers):
	newd = dist(vec, center)
	if newd < curd:
	    curd = newd
	    center_index = i
    return (center_index, (heading, vec))


def getTotals(x, y):
    for i in range(0, numHashes):
	x[1][i] += y[1][i]
    return x
	
	
    

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark-submit kmeans.py <input file> <output file charM> <output file final cluster> <k>", file=sys.stderr)
        exit(-1)
    coeffA = pickRandomCoeffs(numHashes)
    coeffB = pickRandomCoeffs(numHashes)

    sc = SparkContext(appName="K Means")
    lines = sc.wholeTextFiles(sys.argv[1], 1)       
    charM = lines.flatMap(lambda x: re.split(r',business|,sports', x[1]))    
    charM = charM.map(shingler).filter(lambda x: x[0] != -1)
    hashedM = charM.map(hashShingles)

    articleCount = charM.reduceByKey(lambda x,y: (x, 1)).count()

    '''PRINT CHARACTERISTIC MATRIX AS TUPLES'''
    file = open(sys.argv[2], "w")
    for (heading,shingles) in charM.sortBy(lambda x: x[1]).collect():
	shingles.sort()
	for shingle in shingles:
		try:
			file.write("%s,%s" % (shingle, heading))
			file.write("\n")
		except ValueError:
			file.write("ERROR")
    file.close()
    
    '''GET SIGNATURE MATRIX'''
    signatureMatrix = hashedM.map(getSig).cache()
    #signatureMatrix = signatureMatrix.collect()
    
    '''KMEANS'''
    k = int(sys.argv[4])
    centersFromSig = random.sample(signatureMatrix.collect(), k)
    centersFromSig = sc.parallelize(centersFromSig)
    #print(centersFromSig.collect())

    centers = centersFromSig.map(lambda x: x[1]).collect()
    #print(centers)

    cluster = signatureMatrix

    for i in range(0, numIterations):
	for j in range(0, 10):
	    print("H")
	cluster = signatureMatrix.map(lambda x: distMapper(x, centers))
	#print("%s,%s" % ("CLUSTER",cluster.collect())) 

	clusterCounts = cluster.countByKey()
	#print(clusterCounts)
	newCenters = cluster.reduceByKey(getTotals)
	newCenters = newCenters.collect()
	#print("%s,%s" % ("TOTALS:",newCenters))
	centers = []
        for vec in newCenters:
            centers.append(vec[1][1])

	for j in range(0, len(centers)):
	    clustcount = clusterCounts[j]
	    for x in range(0, numHashes):
		centers[j][x] /= clustcount
	#print("%s,%s" % ("CENTERS", centers))
    cluster = cluster.collect()
    '''PRINT FINAL CLUSTERS AS DOCUMENT WITH CLUSTER NAME AND DOC NAME'''
    file = open(sys.argv[3], "w")
    for c in cluster:
            try:
                    file.write("%s,%s" % (c[0], c[1]))
                    file.write("\n")
            except ValueError:
                    file.write("ERROR")
    file.close()

	
	    
    sc.stop()
