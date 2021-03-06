raw_bigrams      = LOAD 'data.txt' using PigStorage('\t') AS (bigram:chararray, year:int, occur:int, books:int);

bigram_gt_100    = FILTER raw_bigrams BY occur > 100;

bigram_freq      = GROUP bigram_gt_100 BY bigram;

flat_bigram_freq = FOREACH bigram_freq GENERATE $0 as bigram, SUM(bigram_gt_100.occur) * 1.0 / SUM(bigram_gt_100.books) AS avg_book;

ordered_bigram = ORDER flat_bigram_freq BY avg_book DESC;

top_bigram = LIMIT ordered_bigram 2;

STORE top_bigram INTO 'test_out.txt';


