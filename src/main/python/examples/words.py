# Load some words
words = sc.textFile("/usr/share/dict/words")
print "{}{}".format("Count of words: ", words.count())
words.take(10)
print "{}{}".format("First word: ", words.first())

# Create a RDD from a List[String]
strings = sc.parallelize(["hello", "world", "hello"])
strings.count()
strings.take(2)
strings.collect()
print "{}".format(strings.collect())

# filter words with "ing"
wordsWithIng = words.filter(lambda line: line.contains("ing"))

# WordCount on words and string using map and reduce
wordsCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
wordsCounts.take(10)
stringsCounts = strings.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
stringsCounts.collect()

# WordCount on shakespeare using map and reduce
shakespeare = sc.textFile("/Users/stuart/git/spark-get-started/src/main/resources/shakespeare.txt")
shakespeareCounts = shakespeare.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
shakespeareCounts.take(10)

