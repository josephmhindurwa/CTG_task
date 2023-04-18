%run ./configurations

# Create function to read csv files into dataframes 
def read_csv_data(csv_path: str) -> DataFrame:
    df = spark.read.format("csv").load(csv_path)
    return (df)

# Create function to filter out bad records 
def remove_bad_records(df: DataFrame) -> DataFrame:
    return (df
            .filter((F.length("ISBN") != 10) & (F.length("ISBN") != 13))
            .filter(F.col("Book-Rating") != 0)
           )

# Merge dataframes on dataset = pd.merge(ratings, books, on=['ISBN'])
def join_book_dataframes(df: DataFrame, df1: DataFrame) -> DataFrame:
    joined_df = df.join(df1, df1.ISBN == df.ISBN ,"left")
    assert df.count() == joined_df.count(), "Different number of rows before and after join"
    return (joined_df)

# Lowercase columns 
def lowercase_column(df: DataFrame, to_lower_case: list) -> DataFrame:
    for col_name in to_lower_case:
        df = df.withColumn(col_name, F.lower(F.col(col_name)))
    return (df)

# Select a specifc book and author 
def get_author_readers(df: DataFrame) -> list:
    author_readers = df.filter((col("Book-Title") == selected_book) & (lower(col("Book-Author")).contains(selected_author)))
    author_readers = author_readers.select("User-ID").rdd.flatMap(lambda x: x).collect() #need to understand more 
    author_readers = list(set(author_readers))
    return author_readers

# consolidate functions 

def clean_data_and_get_tolken_readers (df: DataFrame, df1: DataFrame) -> DataFrame:
    book_ratings_clean_df = remove_bad_records(df)
    bx_books_df_clean_df = remove_bad_records(df1)
    merged_books_df = join_book_dataframes(book_ratings_clean_df, bx_books_df_clean_df)
    df_with_lowercase_cols = lowercase_column(merged_books_df, to_lower_case)
    tolkien_readers = get_author_readers(df_with_lowercase_cols) 
    return (tolkien_readers)
