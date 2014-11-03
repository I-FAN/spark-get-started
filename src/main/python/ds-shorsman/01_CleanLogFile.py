#!/usr/bin/env python

import sys
import itertools
import re
import json
import dateutil.parser

from pyspark import SparkConf, SparkContext

def updateLine(line):
    """
    Parses a JSON record and updates keys and timezone.
    """
    line = line.strip
    line = re.sub("\"itemId\"\"", "\"itemId\"", line)
    line = re.sub("\"created_at\"", "\"createdAt\"", line)
    line = re.sub("\"session_id\"", "\"sessionId\"", line)
    line = re.sub("\"popular\"", "\"popularItems\"", line)
    line = re.sub("\"popularItem\"", "\"popularItems\"", line)
    line = re.sub("\"recommended\"", "\"recommendedItems\"", line)
    line = re.sub("\"recommendedItem\"", "\"recommendedItems\"", line)
    line = re.sub("\"recs\"", "\"recommendedItems\"", line)
    line = re.sub("\"recent\"", "\"recentItem\"", line)
    line = re.sub("\"item_id\"", "\"itemId\"", line)
    
    json_data = json.loads(line)
    json_time = str(json_data['createdAt'])

    d1 = dateutil.parser.parse(json_time)
    d2 = d1.astimezone(dateutil.tz.tzutc())
    d3 = str(d2)
    line = re.sub(json_time, d3, line)
    line = re.sub("\+00:00", "", line) 
    
if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print "Usage: /path/to/spark/bin/spark-submit --driver-memory 2g " + \
          "MovieLensALS.py movieLensDataDir personalRatingsFile"
        sys.exit(1)

rawData = sc.textFile("../../resources/ds-shorsman/rawdata.log")

updatedData = rawData.map(updateLine(line))

updatedData.saveAsTextFile("../../resources/ds-shorsman/weblog.log)

sc.stop()

sys.exit(0)





