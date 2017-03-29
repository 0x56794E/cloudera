A = load 'data.txt' using PigStorage('\n');
B = foreach A generate $0 as id;
store B into 'test.out';
