package com.aamend.hadoop.mapreduce.counter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class CountCdrRecordTypes {

	public static String COUNTER_GROUP_NAME = "Record Types";

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "CountRecordtypes");
		job.setJarByClass(CountCdrRecordTypes.class);
		job.setMapperClass(CountMapper.class);
		job.setNumReduceTasks(0);

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		int code = job.waitForCompletion(false) ? 0 : 1;
		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(
					COUNTER_GROUP_NAME)) {
				System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
			}
		}

		// Clean up empty output directory
		FileSystem.get(conf).delete(output, true);
		System.exit(code);

	}

	public static class CountMapper extends
			Mapper<Object, Text, NullWritable, NullWritable> {

		private Pattern recordTypePattern;

		public void setup(Context context) {
			recordTypePattern = Pattern.compile("[^,]+\\,([0-9]+)\\,(.*)");
		}

		public void map(Object text, Text value, Context context) {

			Matcher m = recordTypePattern.matcher(value.toString());
			if (m.matches()) {
				int recordType = Integer.parseInt(m.group(1));
				context.getCounter(COUNTER_GROUP_NAME,
						String.valueOf(recordType)).increment(1);
			} else {
				context.getCounter(COUNTER_GROUP_NAME, "UNKNOWN").increment(1);
			}

		}

	}

}
