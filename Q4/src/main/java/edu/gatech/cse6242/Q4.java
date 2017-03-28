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
	  String tmpRes = degreeJob(args[0]);
	  frequencyJob(tmpRes, args[1]);
  }
  
  public static void frequencyJob(String inputFile, String outputFile)
			throws IOException, InterruptedException, ClassNotFoundException
  {
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "Q4");
	
	  job.setJarByClass(Q4.class);
	  job.setMapperClass(FrequencyMapper.class);
	  job.setCombinerClass(FrequencyReducer.class);
	  job.setReducerClass(FrequencyReducer.class);
	    
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	    
	  FileInputFormat.addInputPath(job, new Path(inputFile));
	  FileOutputFormat.setOutputPath(job, new Path(outputFile));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  //Return the output filename to pass to next job
  //Tag (append to keep the path) with timestamp so don't have to del files 
  public static String degreeJob(String inputFile)
  				throws IOException, InterruptedException, ClassNotFoundException
  {
	  String outputFile = String.format("%s_%s.tsv", inputFile, System.currentTimeMillis());
	  
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
	  job.waitForCompletion(true);
	  
	  return outputFile;
  }
  
  //Key degree; val count
  public static class FrequencyMapper
  			extends Mapper<Object, Text, Text, IntWritable>
  {
	  private final static IntWritable one = new IntWritable(1);
		
	  @Override
	  public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException
	  {
		  //<node id> <degree>
		  String[] toks = value.toString().split("\\s+");
		  
		  //Skip ill-formatted lines
		  if (toks.length == 2)
		  {
			  context.write(new Text(toks[1]), one);
			  
		  }
	  }
  }

  //Input: key: node id, val: degree
  //Output: key: degree, val: freq
  public static class FrequencyReducer
		extends Reducer<Text, IntWritable, Text, IntWritable>
  {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	
  }
  
  //Mapper<KeyIn, ValueInt, KeyOut, ValueOut>
  //Key: node ID; Value: degree
  public static class DegreeMapper
  			extends Mapper<Object, Text, Text, IntWritable>
  {
	  private final static IntWritable one = new IntWritable(1);
	  	
	  /**
	   * Called once for eavey key/val pair (aka ea line)
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
			  context.write(new Text(toks[0]), one);
			  
			  //Node B
			  context.write(new Text(toks[1]), one);
		  }
	  }
  }
  
  public static class DegreeReducer
  			extends Reducer<Text, IntWritable, Text, IntWritable>
  {
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException 
	  {
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
	  }
  }
}
