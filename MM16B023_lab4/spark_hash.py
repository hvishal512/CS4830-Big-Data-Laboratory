### Written by H.Vishal MM16B023 at 4:02 pm on 22/02/2020 ###

import pyspark
import sys

if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

# A neat way to store the input and output Uris
inputUri=sys.argv[1]
outputUri=sys.argv[2]

# Extract the lines
sc = pyspark.SparkContext()
lines = sc.textFile(sys.argv[1])

# Transform function definition
def transform_func(data):
	date,time,myid = data.split(" ")
	hr,mins,sec = time.split(":")
	if 0 <= int(hr) < 6:
        	return '0-6'
    	elif 6 <= int(hr) < 12:
        	return '6-12'
    	elif 12 <= int(hr) < 18:
        	return '12-18'
    	elif 18 <= int(hr) < 24:
        	return '18-24'

# Input
data = lines.map(transform_func)

# Save output to file
counts = data.map(lambda frame: (frame,1)).reduceByKey(lambda count1, count2: count1 + count2)
counts.saveAsTextFile(sys.argv[2])
