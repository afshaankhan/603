from pyspark.sql import SparkSession
from operator import add
from datetime import datetime
import os

# Date of birth: 23 July 2002

# Initialize SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()


data = spark.sparkContext.textFile("C:/SPARK/603/file1.txt")

# Tokenize and count words
counts = data.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(add)

# Create a timestamp
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

output_path = os.path.join("D:/UMBC/", "output_" + timestamp)
counts.saveAsTextFile(output_path)

