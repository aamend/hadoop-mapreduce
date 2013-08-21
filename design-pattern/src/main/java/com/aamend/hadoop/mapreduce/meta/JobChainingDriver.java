package com.aamend.hadoop.mapreduce.meta;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import com.aamend.hadoop.mapreduce.meta.JobChaining.UserIdBinningMapper;
import com.aamend.hadoop.mapreduce.meta.JobChaining.UserIdCountMapper;
import com.aamend.hadoop.mapreduce.meta.JobChaining.UserIdSumReducer;
import com.aamend.hadoop.mapreduce.meta.ParallelJobs.AverageReputationMapper;
import com.aamend.hadoop.mapreduce.meta.ParallelJobs.AverageReputationReducer;

public class JobChainingDriver {

	public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
	public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "aboveavg";
	public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "belowavg";
	public static final String MULTIPLE_OUTPUTS_ABOVE_5000 = "aboveavg5000";
	public static final String MULTIPLE_OUTPUTS_BELOW_5000 = "belowavg5000";

	public static void main(String[] args) throws Exception {

		// Get CLI path
		Path postInput = new Path(args[0]);
		Path userInput = new Path(args[1]);
		Path countingOutput = new Path(args[3] + "_count");
		Path binningOutputRoot = new Path(args[3] + "_bins");
		Path binningOutputBelow = new Path(binningOutputRoot + "/"
				+ JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME);
		Path binningOutputAbove = new Path(binningOutputRoot + "/"
				+ JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME);
		Path belowAverageRepOutput = new Path(args[2]);
		Path aboveAverageRepOutput = new Path(args[3]);

		// Create first job
		Configuration conf = new Configuration();
		Job countingJob = getCountingJob(conf, postInput, countingOutput);
		int code = 1;

		// Execute first job
		if (countingJob.waitForCompletion(true)) {

			// Create second job that bins result
			ControlledJob binningControlledJob = new ControlledJob(
					getBinningJobConf(countingJob, conf, countingOutput,
							userInput, binningOutputRoot));

			// Create third waves for 2 created bins (below)
			ControlledJob belowAvgControlledJob = new ControlledJob(
					getAverageJobConf(conf, binningOutputBelow,
							belowAverageRepOutput));

			// Create third waves for 2 created bins (above)
			ControlledJob aboveAvgControlledJob = new ControlledJob(
					getAverageJobConf(conf, binningOutputAbove,
							aboveAverageRepOutput));

			// Binning job should be finished
			belowAvgControlledJob.addDependingJob(binningControlledJob);
			aboveAvgControlledJob.addDependingJob(binningControlledJob);

			// Control job execution
			JobControl jc = new JobControl("AverageReputation");
			jc.addJob(binningControlledJob);
			jc.addJob(belowAvgControlledJob);
			jc.addJob(aboveAvgControlledJob);
			jc.run();
			code = jc.getFailedJobList().size() == 0 ? 0 : 1;
		}
		FileSystem fs = FileSystem.get(conf);
		fs.delete(countingOutput, true);
		fs.delete(binningOutputRoot, true);
		System.exit(code);
	}

	public static Job getCountingJob(Configuration conf, Path postInput,
			Path outputDirIntermediate) throws IOException {

		// Setup first job to counter user posts
		Job countingJob = new Job(conf, "JobChaining-Counting");
		countingJob.setJarByClass(JobChainingDriver.class);

		// Set our mapper and reducer
		// we can use the API's long sum reducer for a combiner!
		countingJob.setMapperClass(UserIdCountMapper.class);
		countingJob.setCombinerClass(LongSumReducer.class);
		countingJob.setReducerClass(UserIdSumReducer.class);

		// Setup key / value
		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);

		// Input
		countingJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(countingJob, postInput);

		// Output
		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);

		return countingJob;
	}

	public static Configuration getBinningJobConf(Job countingJob,
			Configuration conf, Path jobchainOutdir, Path userInput,
			Path binningOutput) throws IOException {

		// Calculate the average posts per user by getting counter values
		double numRecords = (double) countingJob
				.getCounters()
				.findCounter(JobChainingDriver.AVERAGE_CALC_GROUP,
						JobChaining.RECORDS_COUNTER_NAME).getValue();
		double numUsers = (double) countingJob
				.getCounters()
				.findCounter(JobChainingDriver.AVERAGE_CALC_GROUP,
						JobChaining.USERS_COUNTER_NAME).getValue();
		double averagePostsPerUser = numRecords / numUsers;

		// Setup binning job
		conf.set(JobChaining.AVERAGE_POSTS_PER_USER,
				Double.toString(averagePostsPerUser));
		Job binningJob = new Job(conf, "JobChaining-Binning");
		binningJob.setJarByClass(JobChainingDriver.class);

		// Set mapper and the average posts per user
		binningJob.setMapperClass(UserIdBinningMapper.class);
		binningJob.setNumReduceTasks(0);

		// Input
		binningJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(binningJob, jobchainOutdir);

		// Add two named outputs for below/above average
		MultipleOutputs.addNamedOutput(binningJob,
				JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(binningJob,
				JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.setCountersEnabled(binningJob, true);

		// Configure multiple outputs
		binningJob.setOutputFormatClass(NullOutputFormat.class);
		FileOutputFormat.setOutputPath(binningJob, jobchainOutdir);
		MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_ABOVE_5000,
				TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(binningJob, MULTIPLE_OUTPUTS_BELOW_5000,
				TextOutputFormat.class, Text.class, LongWritable.class);

		// Add the user files to the DistributedCache
		FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
		for (FileStatus status : userFiles) {
			DistributedCache.addCacheFile(status.getPath().toUri(),
					binningJob.getConfiguration());
		}

		// Execute job and grab exit code
		return binningJob.getConfiguration();
	}

	public static Configuration getAverageJobConf(Configuration conf,
			Path averageOutputDir, Path outputDir) throws IOException {

		// Setup job to get average of user posts
		Job averageJob = new Job(conf, "ParallelJobs");
		averageJob.setJarByClass(ParallelJobs.class);

		// Set our mapper and reducer
		averageJob.setMapperClass(AverageReputationMapper.class);
		averageJob.setReducerClass(AverageReputationReducer.class);

		// Setup key / Value
		averageJob.setOutputKeyClass(Text.class);
		averageJob.setOutputValueClass(DoubleWritable.class);

		// Input
		averageJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(averageJob, averageOutputDir);

		// Output
		averageJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(averageJob, outputDir);

		// Execute job and grab exit code
		return averageJob.getConfiguration();
	}

}
