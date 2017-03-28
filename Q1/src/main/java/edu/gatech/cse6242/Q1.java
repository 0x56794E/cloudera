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

/**
 * 
 * @author Vy Thuy Nguyen
 */
public class Q1 
{

	public static void main(String[] args) throws Exception {
		// Configuration conf = new Configuration();
		// Job job = Job.getInstance(conf, "Q1");
		//
		// /* TODO: Needs to be implemented */
		//
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q1");

		job.setJarByClass(Q1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	//Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> 
	{
		//Key: target node ID; Value: weight
		Map<String, Integer> weightMap = new HashMap<String, Integer>();

		/**
		 * Called once for ea key/value pair in input split
		 * @param key: KeyIn
		 * @param value: ValueIn
		 */
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			//SRC TGT WT
			String[] toks = value.toString().split("\\s+");
			int wt = Integer.parseInt(toks[2]);
			
			//Either the map has NOT had the target node
			//OR it has but the new weight is larger
			if (!weightMap.containsKey(toks[1])
					|| weightMap.get(toks[1]) < wt)
				weightMap.put(toks[1], wt);
		}
		
		/**
		 * Called once at the end of ea task
		 */
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException 
		{
			for (Map.Entry<String, Integer> entry : weightMap.entrySet())
			{
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}

	public static class MaxReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException 
		{
			int max = -1;
			int curVal;
			
			for (IntWritable val : values)
			{
				curVal = val.get();
				if (curVal > max)
					max = curVal;
			}
			
			result.set(max);
			context.write(key, result);
		}
	}
}
