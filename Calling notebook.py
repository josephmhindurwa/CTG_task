
# load in functions and vairables from configurations and operations 
%run ./operations
%run ./configurations

''' takes raw csv files, cleans them out and carrys out aggregations needed to allow recommendation engine to generate book reccomendations '''
#read in csv files using user defined function
book_ratings_raw_df = read_csv_data(book_ratings_path)
bx_books_raw_df = read_csv_data(bx_books_path)

#call functions for transorming data from operations
tolken_readers_df = clean_data_and_get_tolken_readers (book_ratings_raw_df, bx_books_raw_df)

#display data from newly created dataframe
tolken_readers_df.display()
