package com.aamend.hadoop.mapreduce.designpattern.counter;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class CountInternetAccessByMonth {

	public static String COUNTER_GROUP_NAME = "internet_Access";

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String output = null;
		String input = null;

		// Create Hadoop Job
		String jobName = "IACCounterByMonth_" + UUID.randomUUID();
		jobName = jobName.toUpperCase();
		Job job = new Job(conf, jobName);

		// Where to find the MR algo.
		job.setJarByClass(CountInternetAccessByMonth.class);
		job.setMapperClass(CountInternetAccessRecordsPerMonth.class);

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
					COUNTER_GROUP_NAME)) {
				System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
			}
		}

		// Clean up empty output directory
		FileSystem.get(conf).delete(outputPath, true);
		System.exit(code);

	}

	public class CountInternetAccessRecordsPerMonth extends
			Mapper<Object, Text, Object, Object> {

		public void map(Object object, Text text, Context context) {

			String csvLine = text.toString();
			String[] csv = MRDPUtils.csvToStringArray(csvLine);
			String stopTimeStr = csv[0];
			String stopMonthStr = stopTimeStr.substring(0, 6);

			// Count number of records for each month
			// Besides, we know that we will get 6 month of data (6 counter), 12
			// at most
			// Counter are therefore really applicable for that
			context.getCounter(COUNTER_GROUP_NAME, stopMonthStr).increment(1);

		}

	}

}
