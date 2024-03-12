from pyspark.sql import SparkSession
from spellchecker import SpellChecker
from datetime import datetime
import os
import re

# Initialize SparkSession
spark = SparkSession.builder.appName("NonEnglishWordsCount").getOrCreate()

# Initialize SpellChecker with English dictionary
spell = SpellChecker(language='en')

data = spark.sparkContext.textFile("C:/SPARK/603/file2.txt")

# Function to clean and split the text into words
def words_from_line(line):
    # Remove non-alphabetic characters and split line into words
    words = re.sub(r'[^A-Za-z]', ' ', line).split()
    # Filter non-English words
    return [word.lower() for word in words if word.lower() not in spell]

# Tokenize and filter non-English words
non_english_counts = data.flatMap(words_from_line) \
                          .map(lambda word: (word.lower(), 1)) \
                          .reduceByKey(lambda a, b: a + b)

# Collect and print non-English word counts
non_english_counts_collected = non_english_counts.collect()
for word, count in non_english_counts_collected:
    print(word, count)

# Create a unique identifier
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

base_directory = "D:/UMBC/"

output_path = os.path.join(base_directory, "output_" + timestamp)

non_english_counts.saveAsTextFile(output_path)

spark.stop()
