package com.aamend.hadoop.mapreduce.designpattern.counter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class CountNumUserByState {

	public static class CountNumUserByStateDriver {

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();

			String output = null;
			String input = null;

			// Create Hadoop Job
			String jobName = "CounterByState_" + UUID.randomUUID();
			jobName = jobName.toUpperCase();
			Job job = new Job(conf, jobName);

			// Where to find the MR algo.
			job.setJarByClass(CountNumUsersByStateMapper.class);
			job.setMapperClass(CountNumUsersByStateMapper.class);

			// Set up input / output directories
			// Output directory must be set but will contain empty files
			Path outputPath = new Path(output + "/" + jobName);
			Path inputPath = new Path(input);
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.out.println("*****************************");
			System.out.println("JobName : \t" + jobName);
			System.out.println("Output Path : \t" + output + "/" + jobName);
			System.out.println("Input Path : \t" + input);
			System.out.println("*****************************");

			// keep a synchronous Hadoop call
			int code = job.waitForCompletion(true) ? 0 : 1;
			if (code == 0) {
				// Job successfully processed, retrieve counters
				for (Counter counter : job.getCounters().getGroup(
						CountNumUsersByStateMapper.STATE_COUNTER_GROUP)) {
					System.out.println(counter.getDisplayName() + "\t"
							+ counter.getValue());
				}
			}

			// Clean up empty output directory
			FileSystem.get(conf).delete(outputPath, true);
			System.exit(code);

		}

	}

	public class CountNumUsersByStateMapper extends
			Mapper<Object, Text, NullWritable, NullWritable> {

		public static final String STATE_COUNTER_GROUP = "State";
		public static final String UNKNOWN_COUNTER = "Unknown";
		public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";
		private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
				"CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
				"IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
				"MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
				"OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
				"VT", "VA", "WA", "WV", "WI", "WY" };
		private HashSet<String> states = new HashSet<String>(
				Arrays.asList(statesArray));

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// Get the value for the Location attribute
			String location = parsed.get("Location");
			// Look for a state abbreviation code if the
			// location is not null or empty
			if (location != null && !location.isEmpty()) {
				// Make location uppercase and split on white space
				String[] tokens = location.toUpperCase().split("\\s");
				// For each token
				boolean unknown = true;
				for (String state : tokens) {
					// Check if it is a state
					if (states.contains(state)) {
						// If so, increment the state's counter by 1
						// and flag it as not unknown
						context.getCounter(STATE_COUNTER_GROUP, state)
								.increment(1);
						unknown = false;
						break;
					}
				}
				// If the state is unknown, increment the UNKNOWN_COUNTER
				// counter
				if (unknown) {
					context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER)
							.increment(1);
				}
			} else {
				// If it is empty or null, increment the
				// NULL_OR_EMPTY_COUNTER counter by 1
				context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER)
						.increment(1);
			}
		}
	}
}
