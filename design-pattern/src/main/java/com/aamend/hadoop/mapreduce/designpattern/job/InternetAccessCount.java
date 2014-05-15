package com.aamend.hadoop.mapreduce.designpattern.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InternetAccessCount {

	/**
	 * 
	 * 1. Export project hadoop Map Reduce as a jar file 2. scp jar file to
	 * hadoop jobtracker 3. Execute
	 * "hadoop jar %JAR%.jar com.aamend.hadoop.mapreduce.job.InternetAccessDriver"
	 * 
	 * Arguments are: --------------
	 * 
	 * outputPathDir = args[0] => output directory where output files will be
	 * stored inputPathDir = args[1] => input directory where files will be read
	 * targetValue = args[2] => ip address to lookup fromDate = args[3] => Min
	 * Date for query (DDMMYYYYHHMISS) toDate = args[4] => Max Date for query
	 * (DDMMYYYYHHMISS) priority = args[5] => Hadoop priority (0-4)
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		String output = null;
		String input = null;
		String targetValue = null;
		String fromDate = null;
		String toDate = null;
		String priorityString = null;
		String hadoopPriority;
		int priority = 0;

		// Retrieve input parameters
		try {

			output = otherArgs[0];
			input = otherArgs[1];
			targetValue = otherArgs[2];
			fromDate = otherArgs[3];
			toDate = otherArgs[4];
			priorityString = otherArgs[5];

		} catch (ArrayIndexOutOfBoundsException e) {
			System.exit(1);
		}

		priority = Integer.valueOf(priorityString);
		switch (priority) {
		case 0:
			hadoopPriority = "VERY_LOW";
			break;
		case 1:
			hadoopPriority = "LOW";
			break;
		case 2:
			hadoopPriority = "NORMAL";
			break;
		case 3:
			hadoopPriority = "HIGH";
			break;
		case 4:
			hadoopPriority = "VERY_HIGH";
			break;
		default:
			hadoopPriority = "NORMAL";
			break;
		}

		// Set up Hadoop conf.
		conf.set("targetValue", targetValue);
		conf.set("fromDate", fromDate);
		conf.set("toDate", toDate);
		conf.set("mapred.job.priority", hadoopPriority);

		// Create Hadoop Job
		String jobName = "InternetAccess_" + UUID.randomUUID();
		jobName = jobName.toUpperCase();
		Job job = new Job(conf, jobName);

		// Where to find the MR algo.
		job.setJarByClass(InternetAccessCount.class);
		job.setMapperClass(InternetAccessCountMapper.class);
		job.setCombinerClass(InternetAccessCountReducer.class);
		job.setReducerClass(InternetAccessCountReducer.class);

		// Signature of the key value Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Create Sequence format
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		// Set up input / output directories
		Path outputPath = new Path(output + "/" + jobName);
		Path inputPath = new Path(input);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.out.println("*****************************");
		System.out.println("JobName : \t" + jobName);
		System.out.println("Target Value : \t" + targetValue);
		System.out.println("From Date : \t" + fromDate);
		System.out.println("To Date : \t" + toDate);
		System.out.println("Output Path : \t" + output + "/" + jobName);
		System.out.println("Input Path : \t" + input);
		System.out.println("Priority : \t" + hadoopPriority);
		System.out.println("*****************************");

		// keep a synchronous Hadoop call
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

		// keep an asynchronous Hadoop call
		// job.submit();

	}

	public class InternetAccessCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private String targetValue;
		private String toDate;
		private String fromDate;
		private HashMap<String, String> csv;

		/**
		 * MAP scope Count the time of the day where IP=$IP was connecting the
		 * most often Find IP with correct time stamp
		 * 
		 * @inputKey: none
		 * @InputValue: CSV line
		 * @outputKey: time of the day
		 * @outputValue : Counter
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			// I. Retrieve parameters from query
			Configuration conf = context.getConfiguration();
			targetValue = conf.get("targetValue");
			fromDate = conf.get("fromDate");
			toDate = conf.get("toDate");

			// II. Pre-Query
			if (!line.contains(targetValue))
				return;

			// III. Parse CSV and populate HashMap
			csv = new HashMap<String, String>(
					InternetAccessParameters.MY_CSV_ARRAY.length);
			int i = 0;
			try {
				for (String column : MRDPUtils.csvToStringArray(value
						.toString())) {
					csv.put(InternetAccessParameters.MY_CSV_ARRAY[i], column);
					i++;
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				return;
			}

			String ip = csv.get("CLIENT_IP");
			if (ip == null)
				return;

			String stopTime = csv.get("STOP_TIME");
			if (stopTime == null)
				return;

			// IV. Query Target
			if (!ip.equals(targetValue))
				return;

			// V. Query Date
			if (stopTime.compareTo(fromDate) < 0
					|| stopTime.compareTo(toDate) > 0)
				return;

			// VI. Extract hour of the day
			String strTime = stopTime.substring(8, 10);
			Text text = new Text(strTime);

			// VII. Output time of the day and counter
			context.write(text, new IntWritable(1));

		}

	}

	public class InternetAccessCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * REDUCE scope Count the time of the day where IP=$IP was connecting
		 * the most often Find IP with correct timestamp
		 * 
		 * @inputKey: Hour of the day
		 * @InputValue: Counter
		 * @outputKey: Hour of the day
		 * @outputValue : Counter
		 */
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			// Iterate through hour of the day and sum up counter
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	public interface InternetAccessParameters {

		// CSV mapping
		public String[] MY_CSV_ARRAY = new String[] { "STOP_TIME",
				"START_TIME", "CLIENT_IP", "ACCESS_UNIT", "HOSTNAME",
				"INPUT_BYTES", "OUTPUT_BYTES", "CALLED_ST_ID", "SESSION_ID",
				"PARTITION", };

	}

}
