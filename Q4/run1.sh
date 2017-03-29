hadoop jar ./target/q4-1.0.jar edu.gatech.cse6242.Q4 /user/cse6242/small.tsv /user/cse6242/localsmalloutput
hadoop fs -getmerge /user/cse6242/localsmalloutput/ localsmalloutput.tsv
hadoop fs -rm -r /user/cse6242/localsmalloutput
