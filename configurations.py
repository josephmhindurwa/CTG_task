#call libraries used in code 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Assign path vairables to csv files
book_ratings_path = ("/Users/joe/Downloads/book_recommender/BX-Book-Ratings.csv")
bx_books_path = ("/Users/joe/Downloads/book_recommender/BX-Books.csv")
to_lower_case = ["User-ID", "Book-Title", "Book-Author"]
selected_book = "the fellowship of the ring (the lord of the rings, part 1)"
selected_author = "tolkien"
