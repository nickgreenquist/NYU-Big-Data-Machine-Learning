from __future__ import print_function

import sys
import operator
from operator import add

from pyspark import SparkContext


n = 10

def mapper(x):
    pair = x.split()
    user = pair[0]
    if len(pair) < 2:
	return [((user, user), -1000)]
    friends = pair[1]
    friends = friends.split(",")
    actualFriends = [((user, f), -1000) for f in friends]
    c = []
    for i in range(0, len(friends)):
	for j in range(i + 1, len(friends)):
		c.append((friends[i], friends[j]))
		c.append((friends[j], friends[i])) 
    commonFriends = [(p, 1) for p in c]
    return actualFriends + commonFriends

def mapSuggestions(kvp):
    user = kvp[0]
    suggestions = kvp[1]
    suggestionCounts = {}
    for s in suggestions:
	if s[0] > 0:
		suggestionCounts[s[1]] = s[0]
    suggestionCounts = sorted(suggestionCounts.items(), key=lambda x: (-x[1], x[0]))
    
    suggestionCounts = suggestionCounts[:n]
    return (user, suggestionCounts)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit friends.py <input friends file> <output file>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Friend Reccomendations")

    lines = sc.textFile(sys.argv[1], 1)   

    pairs = lines.flatMap(mapper)

    pairsReduced = pairs.reduceByKey(lambda x, y: x + y)

    friendCounts = pairsReduced.map(lambda ((u, f), num): (u, (num, f)))

    friendCounts = friendCounts.groupByKey()
   
    suggestions = friendCounts.map(mapSuggestions)

    suggestions.sortByKey().saveAsTextFile(sys.argv[2])	

    sc.stop()
