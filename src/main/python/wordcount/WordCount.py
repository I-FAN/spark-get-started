# Create new SparkContext
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

inFile = "../../resources/wordcount/shakespeare.txt"
outFile = "./counts"

# WordCount on shakespeare using map and reduce
shakespeare = sc.textFile(inFile)
shakespeareCounts = shakespeare.flatMap(lambda line: line.split(" ")) \
                                .map(lambda word: word.replace("[^a-zA-Z]", "")) \
                                .map(lambda word: (word, 1)) \
                                .reduceByKey(lambda a, b: a + b)

shakespeareCounts.saveAsTextFile(outFile)
