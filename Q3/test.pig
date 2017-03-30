bigrams = load 'data.txt' using PigStorage('\t') AS (bigram:chararray, year:int, occurr:int, books:int);
B = foreach bigrams generate $0 as id;
store B into 'test.out';
