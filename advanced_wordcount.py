import argparse
import csv
import os
from pyspark.sql import SparkSession
from nltk.tokenize import word_tokenize
from string import punctuation
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer


def get_args():
    parser = argparse.ArgumentParser('pyspark wordcount')
    parser.add_argument('--data', required=True, help='dir path of I/O data')
    parser.add_argument('--read', required=True, help='file name to read and process')
    return parser.parse_args()


def create_review_txtfile(dir_path, input_file_name):
    # Read csv file, extract a column 'review/text', and export to a txt file
    input_file_path = os.path.join(dir_path, input_file_name)
    outout_file_path = os.path.join(dir_path, 'original_text.text')

    with open(input_file_path, 'r') as ifp:
        with open(outout_file_path, 'w') as ofp:
            reader = csv.reader(ifp, delimiter=',', quotechar='"')
            cols = next(reader)
            for row in reader:
                ofp.write(row[9] + '\n')


def process_text(dir_path):
    # Read the review text file
    input_file_path  = os.path.join(dir_path, 'original_text.text')
    with open(input_file_path, 'r') as ifp:
        text = ifp.read()
    # Create sets of stop words and punctuation
    stop_word_set = set(stopwords.words('english'))
    punct_word_set = set(punctuation)
    punct_word_set.update(["``", "''", "...", "..", "--"])
    # Initialize a stemmer
    snow = SnowballStemmer("english")
    # Tokenize the sentences
    tokens = word_tokenize(text)
    # Process stemming (i.e., convert words into their root form)
    tokens = [snow.stem(token.lower()) for token in tokens]
    # Clean the tokens by removing stop words and punctuation
    cleaned_tokens = \
        [token for token in tokens if token not in stop_word_set and token not in punct_word_set]
    outout_text = " ".join(cleaned_tokens)
    outout_file_path = os.path.join(dir_path, 'tokenized_text.txt')
    with open(outout_file_path, 'w') as ofp:
        ofp.write(outout_text)


def run_spark(dir_path):
    input_file_path = os.path.join(dir_path, 'tokenized_text.txt')
    # Create SparkSession
    spark = SparkSession.builder.appName('BookReviews').getOrCreate()
    # Read the text file and return it as an RDD (Resilient Distributed Datasets)
    rdd = spark.sparkContext.textFile(input_file_path)
    # Split by space and return a new RDD
    rdd2 = rdd.flatMap(lambda x: x.split(" "))
    # Return key-value pairs, word type String as Key and 1 of type int as value
    rdd3 = rdd2.map(lambda x: (x, 1))
    # Merge the values of each key
    rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
    # Sort the word count by descending order
    rdd5 = rdd4.map(lambda x: (x[1], x[0])).sortByKey(False)
    # Show as (key, value) format
    rdd6 = rdd5.map(lambda x: (x[1], x[0]))
    output_file_path = os.path.join(dir_path, 'final_output.txt')
    with open(output_file_path, 'w') as ofp:
        for element in rdd6.collect():
            ofp.write(str(element) + '\n')


def main(args):
    dir_path = args.data
    input_file_path = args.read
    create_review_txtfile(dir_path, input_file_path)
    process_text(dir_path)
    run_spark(dir_path)  


if __name__ == '__main__':
    main(get_args())
