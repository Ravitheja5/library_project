from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = "python"

spark = SparkSession.builder.appName("LibraryProject").getOrCreate()
sc = spark.sparkContext

# Load data
books = sc.textFile("data/books.csv")
transactions = sc.textFile("data/transactions.csv")

# Remove header
books_rdd = books.map(lambda x: x.split(",")).filter(lambda x: x[0] != "book_id")
trans_rdd = transactions.map(lambda x: x.split(",")).filter(lambda x: x[0] != "trans_id")

# Count borrowed books
book_counts = trans_rdd.map(lambda x: (x[2], 1)) \
                       .reduceByKey(lambda a, b: a + b)

print("Most Borrowed Books:", book_counts.collect())