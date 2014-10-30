val words = sc.textFile("/usr/share/dict/words")
words.count()
words.take(10)