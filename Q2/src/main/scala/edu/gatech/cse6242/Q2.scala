package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Q2 
{
	//MUST BE PLACED HERE
	//NOT in the same method it's used!!! (God knows why)
	case class Edge (src: String, tgt: String, weight: Int)
	
	def main(args: Array[String]) 
	{
    	val sc = new SparkContext(new SparkConf().setAppName("Q2"))
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

    	// read the file
    	//=> get this as RDD
    	val file = sc.textFile("hdfs://localhost:8020" + args(0))
		
		/* TODO: Needs to be implemented */
		
		//create data frame
		val dataFrame = file.map(_.split("\\s+")).map(ed => Edge(ed(0), ed(1), ed(2).trim.toInt)).toDF()
		
		//Filter for edges with weight != 1
		//Outgoing weight
		val outDF = dataFrame.filter(dataFrame("weight") > 1)
						.groupBy(dataFrame("src"))
						.agg(sum("weight").as("outW"))
						.as("out_df")
			
		val inDF = dataFrame.filter(dataFrame("weight") > 1)
						.groupBy(dataFrame("tgt"))
						.agg(sum("weight").as("inW"))
						.as("in_df")
			
		val joinDF = outDF.join(inDF, outDF("src") === inDF("tgt"), "outer")
						  .na.fill(Map("outW" -> 0, "inW" -> 0))
						  .withColumn("W", col("inW") - col("outW"))
		val retDF = joinDF.select(coalesce(joinDF("src"), joinDF("tgt")), joinDF("W"))
			 
	    retDF.show()
						
    	// store output on given HDFS path.
    	// YOU NEED TO CHANGE THIS
    	retDF.rdd.saveAsTextFile("hdfs://localhost:8020" + args(1))
  	}
}
