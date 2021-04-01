#!/usr/bin/env python

import pyspark
import sys

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

inputUri=sys.argv[1]
outputUri=sys.argv[2]

sc = pyspark.SparkContext()

lines = sc.textFile(sys.argv[1])
temp = lines.flatMap(lambda line: [(line.split())[1]]).filter(lambda word: word[0].isdigit())
words = temp.map(lambda word: '-'.join([str((6*(int(word[0:2])/6))),str(((6*(int(word[0:2])/6)))+int(6))]))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
wordCounts.coalesce(1).saveAsTextFile(sys.argv[2])
