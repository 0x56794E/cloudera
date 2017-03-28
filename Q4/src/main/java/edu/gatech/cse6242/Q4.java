package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 
{
  public static void main(String[] args) throws Exception 
  {
	  degreeJob(args[0], args[1]);
	  
  }
  
  //Return the output filename to pass to next job
  //Tag (append to keep the path) with timestamp so don't have to del files 
  public static void degreeJob(String inputFile, String outputFile)
  				throws IOException, InterruptedException, ClassNotFoundException
  {
	  ///String outputFile = String.format("%s_%s.tsv", inputFile, System.currentTimeMillis());
	  //String outputFile = "/user/cse6242/q4outputsm"; //tmp
	  
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "Q4");
	
	  job.setJarByClass(Q4.class);
	  job.setMapperClass(DegreeMapper.class);
	  job.setCombinerClass(DegreeReducer.class);
	  job.setReducerClass(DegreeReducer.class);
	    
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	    
	  FileInputFormat.addInputPath(job, new Path(inputFile));
	  FileOutputFormat.setOutputPath(job, new Path(outputFile));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	  //return outputFile;
  }
  
  //Mapper<KeyIn, ValueInt, KeyOut, ValueOut>
  public static class DegreeMapper
  			extends Mapper<Object, Text, Text, IntWritable>
  {
	  //Key: node ID; Value: degree
	  Map<String, Integer> degMap = new HashMap<String, Integer>();
	  
	  /**
	   * Called once for eavey key/val pair
	   */
	  @Override
	  public void map(Object key, Text value, Context context)
  				throws IOException, InterruptedException
	  {
		  //<node 1> <node 2>
		  String[] toks = value.toString().split("\\s+");
		  
		  //Skip ill-formatted lines
		  if (toks.length == 2)
		  {
			  //Check BOTH nodes
			  
			  //Node A
			  if (!degMap.containsKey(toks[0]))
				  degMap.put(toks[0], 0);
	
			  degMap.put(toks[0], degMap.get(toks[0]) + 1);
			  
			  //Node B
			  if (!degMap.containsKey(toks[1]))
				  degMap.put(toks[1], 0);
			  
			  degMap.put(toks[1], degMap.get(toks[1]) + 1);
		  }
	  }
	  
	  /**
	   * Called once at the end of ea task
	   */
	  @Override
	  public void cleanup(Context context)
	  		throws IOException, InterruptedException
	  {
		  for (Map.Entry<String, Integer> entry : degMap.entrySet())
		  {	
			  context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));			
		  }
	  }
  }
  
  public static class DegreeReducer
  			extends Reducer<Text, IntWritable, Text, IntWritable>
  {
	  
  }
}
