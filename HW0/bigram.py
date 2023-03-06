import sys
from pyspark import SparkContext, SparkConf

# Create Spark context. 
conf = SparkConf()
sc = SparkContext(conf = conf)

# Build Bigrams. 
def word_pairs(l):
    ws = l.split()
    return [w1 + " " + w2 for w1, w2 in zip(ws[:-1], ws[1:]) if w1 != "" and w2 != ""]

# Clean text from non-alphabetic chars.
words = sc.textFile(sys.argv[1]) \
          .flatMap(lambda line: line.lower().replace("[^a-z]+", " ").split(" "))

# Build dictionary of word counts.
word_counts_dict = words.map(lambda word: (word, 1)) \
                        .reduceByKey(lambda a, b: a + b).collectAsMap()

bigrams = sc.textFile(sys.argv[1]).flatMap(word_pairs) 

# Compute percentage of bigram occurrence
bigrams_count = bigrams \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda a, b: a + b)

bigrams_freq = bigrams_count \
                 .map(lambda x: (x[0], x[1] / float(word_counts_dict[x[0].split(" ")[0]])))


# Finally save the output to another text file
bigrams_count.sortBy(lambda x: x[0]) \
             .coalesce(1, shuffle = False) \
             .saveAsTextFile(sys.argv[2])

bigrams_freq.sortBy(lambda x: x[0]) \
            .coalesce(1, shuffle = False) \
            .saveAsTextFile(sys.argv[3])
sc.stop()


