package com.aamend.hadoop.mapreduce.shuffling;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Anonymize {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Anonymize");

		job.setJarByClass(Anonymize.class);
		job.setMapperClass(AnonymizeMapper.class);
		job.setReducerClass(AnonymizeReducer.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(10);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);

	}

}
