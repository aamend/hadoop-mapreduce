package com.aamend.hadoop.mapreduce.designpattern.meta;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ParallelJobs {

	public static void main(String[] args) throws Exception {

		Path belowAvgInputDir = new Path(args[0]);
		Path aboveAvgInputDir = new Path(args[1]);
		Path belowAvgOutputDir = new Path(args[2]);
		Path aboveAvgOutputDir = new Path(args[3]);

		Job belowAvgJob = submitJob(belowAvgInputDir, belowAvgOutputDir);
		Job aboveAvgJob = submitJob(aboveAvgInputDir, aboveAvgOutputDir);
		// While both jobs are not finished, sleep

		while (!belowAvgJob.isComplete() || !aboveAvgJob.isComplete()) {
			Thread.sleep(5000);
		}

		if (belowAvgJob.isSuccessful()) {
			System.out.println("Below average job completed successfully!");
		} else {
			System.out.println("Below average job failed!");
		}

		if (aboveAvgJob.isSuccessful()) {
			System.out.println("Above average job completed successfully!");
		} else {
			System.out.println("Above average job failed!");
		}

		System.exit(belowAvgJob.isSuccessful() && aboveAvgJob.isSuccessful() ? 0
				: 1);
	}

	public static class AverageReputationMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static final Text GROUP_ALL_KEY = new Text(
				"Average Reputation:");

		private DoubleWritable outvalue = new DoubleWritable();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the line into tokens
			String[] tokens = value.toString().split("\t");
			// Get the reputation from the third column
			double reputation = Double.parseDouble(tokens[2]);
			// Set the output value and write to context
			outvalue.set(reputation);
			context.write(GROUP_ALL_KEY, outvalue);
		}
	}

	public static class AverageReputationReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable outvalue = new DoubleWritable();

		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0;
			for (DoubleWritable dw : values) {
				sum += dw.get();
				++count;
			}
			outvalue.set(sum / count);
			context.write(key, outvalue);
		}
	}

	private static Job submitJob(Path inputDir, Path outputDir)
			throws Exception {

		// Create job & conf
		Configuration conf = new Configuration();
		Job job = new Job(conf, "ParallelJobs");
		job.setJarByClass(ParallelJobs.class);

		// Setup mapper and reducer
		job.setMapperClass(AverageReputationMapper.class);
		job.setReducerClass(AverageReputationReducer.class);

		// Setup key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// Input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inputDir);
		
		// Output
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);
		
		// Submit job and immediately return, rather than waiting for completion
		job.submit();
		return job;
	}
}
