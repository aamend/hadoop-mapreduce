package com.aamend.hadoop.mapreduce.designpattern.sorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalOrderSorting {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path inputPath = new Path(args[0]);
		Path partitionFile = new Path(args[1] + "_partitions.lst");
		Path outputStage = new Path(args[1] + "_staging");
		Path outputOrder = new Path(args[1]);

		// Configure job to prepare for sampling
		// Output any line to column2sort + line 
		Job sampleJob = new Job(conf, "TotalOrderSortingStage");
		sampleJob.setJarByClass(TotalOrderSorting.class);
		
		// Use the mapper implementation with zero reduce tasks
		sampleJob.setMapperClass(IacStopTimeMapper.class);
		sampleJob.setNumReduceTasks(0);
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		TextInputFormat.setInputPaths(sampleJob, inputPath);
		
		// Set the output format to a sequence file
		sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);

		int code = sampleJob.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			
			//Now that we have extracted column to sort
			
			
			Job orderJob = new Job(conf, "TotalOrderSortingStage");
			orderJob.setJarByClass(TotalOrderSorting.class);
			// Here, use the identity mapper to output the key/value pairs in
			// the SequenceFile
			orderJob.setMapperClass(Mapper.class);
			orderJob.setReducerClass(ValueReducer.class);
			// Set the number of reduce tasks to an appropriate number for the
			// amount of data being sorted
			orderJob.setNumReduceTasks(10);
			// Use Hadoop's TotalOrderPartitioner class
			orderJob.setPartitionerClass(TotalOrderPartitioner.class);
			// Set the partition file
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),
					partitionFile);
			orderJob.setOutputKeyClass(Text.class);
			orderJob.setOutputValueClass(Text.class);
			// Set the input to the previous job's output
			orderJob.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
			// Set the output path to the command line parameter
			TextOutputFormat.setOutputPath(orderJob, outputOrder);
			// Set the separator to an empty string
			orderJob.getConfiguration().set(
					"mapred.textoutputformat.separator", "");
			// Use the InputSampler to go through the output of the previous
			// job, sample it, and create the partition file
			InputSampler.writePartitionFile(orderJob,
					new InputSampler.RandomSampler(.1, 10000));
			// Submit the job
			code = orderJob.waitForCompletion(true) ? 0 : 2;
		}

		// Clean up the partition file and the staging directory
		// FileSystem.get(new Configuration()).delete(partitionFile, false);
		// FileSystem.get(new Configuration()).delete(outputStage, true);
		System.exit(code);

	}
}
