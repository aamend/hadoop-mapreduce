package com.aamend.hadoop.mapreduce.designpattern.meta;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class JobChaining {

	public static final String AVERAGE_CALC_GROUP = "average.calc.group";
	public static final String AVERAGE_POSTS_PER_USER = "avg.posts.per.user";
	public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "multiple.below";
	public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "multiple.above";
	public static final String RECORDS_COUNTER_NAME = "Records";
	public static final String USERS_COUNTER_NAME = "Users";

	public static void main(String[] args) throws Exception {

		// Setup I/O path
		Path postInput = new Path(args[0]);
		Path userInput = new Path(args[1]);
		Path outputDirIntermediate = new Path(args[2] + "_int");
		Path outputDir = new Path(args[2]);

		// Setup first job to counter user posts
		Configuration confCounting = new Configuration();
		Job countingJob = new Job(confCounting, "JobChaining-Counting");
		countingJob.setJarByClass(JobChaining.class);

		// Set our mapper and reducer
		// we can use the API's long sum reducer for a combiner!
		countingJob.setMapperClass(UserIdCountMapper.class);
		countingJob.setCombinerClass(LongSumReducer.class);
		countingJob.setReducerClass(UserIdSumReducer.class);

		// Specify Key / Value
		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(LongWritable.class);

		// Input
		countingJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(countingJob, postInput);

		// Output
		countingJob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);

		// Execute job and grab exit code
		int code = countingJob.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {

			// Calculate the average posts per user by getting counter values
			double numRecords = (double) countingJob.getCounters()
					.findCounter(AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME)
					.getValue();

			double numUsers = (double) countingJob.getCounters()
					.findCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME)
					.getValue();

			double averagePostsPerUser = numRecords / numUsers;

			// Setup binning job
			Configuration confBinning = new Configuration();
			confBinning.set(AVERAGE_POSTS_PER_USER,
					Double.toString(averagePostsPerUser));
			Job binningJob = new Job(confBinning, "JobChaining-Binning");
			binningJob.setJarByClass(JobChaining.class);

			// Set mapper and the average posts per user
			binningJob.setMapperClass(UserIdBinningMapper.class);
			binningJob.setNumReduceTasks(0);

			// Input
			binningJob.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.addInputPath(binningJob, outputDirIntermediate);

			// Add two named outputs for below/above average
			MultipleOutputs.addNamedOutput(binningJob,
					MULTIPLE_OUTPUTS_BELOW_NAME, TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(binningJob,
					MULTIPLE_OUTPUTS_ABOVE_NAME, TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.setCountersEnabled(binningJob, true);
			TextOutputFormat.setOutputPath(binningJob, outputDir);

			// Add the user files to the DistributedCache
			FileStatus[] userFiles = FileSystem.get(confCounting).listStatus(
					userInput);
			for (FileStatus status : userFiles) {
				DistributedCache.addCacheFile(status.getPath().toUri(),
						binningJob.getConfiguration());
			}

			// Execute job and grab exit code
			code = binningJob.waitForCompletion(true) ? 0 : 1;

		}

		// Clean up the intermediate output
		FileSystem.get(confCounting).delete(outputDirIntermediate, true);
		System.exit(code);

	}

	public class UserIdBinningMapper extends Mapper<Object, Text, Text, Text> {

		private double average = 0.0;
		private MultipleOutputs<Text, Text> mos = null;
		private Text outkey = new Text();
		private Text outvalue = new Text();
		private HashMap<String, String> userIdToReputation = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			average = Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
			userIdToReputation = new HashMap<String, String>();
			mos = new MultipleOutputs<Text, Text>(context);

			// Read all files in the DistributedCache
			Path[] files = DistributedCache.getLocalCacheFiles(conf);
			for (Path p : files) {
				BufferedReader rdr = new BufferedReader(new InputStreamReader(
						new GZIPInputStream(new FileInputStream(new File(
								p.toString())))));
				String line;
				// For each record in the user file
				while ((line = rdr.readLine()) != null) {
					// Get the user ID and reputation
					Map<String, String> parsed = MRDPUtils.xmlToMap(line);
					// Map the user ID to the reputation
					userIdToReputation.put(parsed.get("Id"),
							parsed.get("Reputation"));
				}
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\t");

			String userId = tokens[0];
			int posts = Integer.parseInt(tokens[1]);

			outkey.set(userId);
			outvalue.set((long) posts + "\t" + userIdToReputation.get(userId));
			if ((double) posts < average) {
				mos.write(MULTIPLE_OUTPUTS_BELOW_NAME, outkey, outvalue,
						MULTIPLE_OUTPUTS_BELOW_NAME + "/part");
			} else {
				mos.write(MULTIPLE_OUTPUTS_ABOVE_NAME, outkey, outvalue,
						MULTIPLE_OUTPUTS_ABOVE_NAME + "/part");
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}

	public class UserIdCountMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		private Text outkey = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			String userId = parsed.get("OwnerUserId");
			if (userId != null) {

				// Output userId and "1"
				outkey.set(userId);
				context.write(outkey, new LongWritable(1));

				// Increment counter "records"
				context.getCounter(AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME)
						.increment(1);
			}
		}
	}

	public class UserIdSumReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable outvalue = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			// Increment user counter, as each reduce group represents one user
			context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME)
					.increment(1);

			int sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}

			outvalue.set(sum);
			context.write(key, outvalue);

		}
	}

}
