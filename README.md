# Apache Spark Get Started 

This is a spark SBT Template.

To use it, import it into IntelliJ or your favorite IDE as an SBT project.

Then you should be able to run standard SBT tests/compile tasks etc 
inside your idea and also in standalone mode.

## Running the WordCount Example
```
$ cd <git home>/spark-get-started/src/main/python/wordcount
$ rm -rf counts
$ ./runit.sh
```
counts the number of words written by Shakespeare and writes the results into ./counts.  Uses a local master.

## Running the Movie Recommender (ALS) Example
```
$ cd <git home>/spark-get-started/src/main/python/als
$ ./runit.sh
```
asks you to rate some movies, which will be input with other user ratings to generate the recommendations.  Borrowed from my Spark training when I was at Cloudera.  Uses a local master.
 
