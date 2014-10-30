package wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount extends App {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf) 

    val in = "/Users/stuart/git/spark-get-started/src/main/resources/shakespeare.txt"
    val out = "/Users/stuart/git/spark-get-started/src/main/resources/shakespeare.counts"

    val shakespeare = sc.textFile(in)
    val shakespeareCounts = shakespeare.flatMap(line => line.split(" "))
        .map(word => word.replaceAll("[^a-zA-Z]", ""))
        .map(word => (word, 1))
        .reduceByKey{case (x, y) => x + y}

    shakespeareCounts.saveAsTextFile(out) 

    sc.stop()
    sys.exit()
}
