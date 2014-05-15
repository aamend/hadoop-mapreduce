package com.aamend.hadoop.mapreduce.designpattern.partitioning;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class PartitionByDate {

	// This object will format the creation date string into a Date object
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");

	public static void mail(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "PostCommentHierarchy");
		job.setJarByClass(PartitionByDate.class);

		job.setMapperClass(LastAccessDateMapper.class);
		
		// Set up input / output directories
		// Output directory must be set but will contain empty files
		Path outputPath = new Path(args[0]);
		Path inputPath = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		// Set custom partitioner and min last access date
		job.setPartitionerClass(LastAccessDatePartitioner.class);
		LastAccessDatePartitioner.setMinLastAccessDate(job, 2008);

		// Last access dates span between 2008-2011, or 4 years
		// Should get as much reducers as you will get partition, no more, no
		// less !
		// Basically 1 reducer will output 1 file to a specific partition folder
		job.setNumReduceTasks(4);

		job.waitForCompletion(true);

	}

	public static class LastAccessDateMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private IntWritable outkey = new IntWritable();

		public void map(Object text, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// Grab the last access date
			String strDate = parsed.get("LastAccessDate");
			// Parse the string into a Calendar object
			Calendar cal = Calendar.getInstance();
			try {
				cal.setTime(frmt.parse(strDate));
			} catch (ParseException e) {
				return;
			}
			outkey.set(cal.get(Calendar.YEAR));
			// Write out the year with the input value
			context.write(outkey, value);
		}

	}

	public static class LastAccessDatePartitioner extends
			Partitioner<IntWritable, Text> implements Configurable {

		private Configuration conf = null;
		private int minLastAccessDateYear = 0;

		private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";

		public static void setMinLastAccessDate(Job job,
				int minLastAccessDateYear) {
			job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,
					minLastAccessDateYear);
		}

		@Override
		// All users who last logged in 2008 will be assigned to partition zero
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			return key.get() - minLastAccessDateYear;
		}

		@Override
		// The setConf method is called during task construction to configure
		// the partitioner
		public void setConf(Configuration conf) {
			this.conf = conf;
			minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

	}

	public static class ValueReducer extends
			Reducer<IntWritable, Text, Text, NullWritable> {
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, NullWritable.get());
			}
		}
	}

}
