package com.aamend.hadoop.mapreduce.designpattern.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortAndPartition {

	private static int MAX_USER_ID = 10;

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		
		Path userInputPath = new Path(args[0]);
		Path userOutputPath = new Path(args[1]);
		Path iacInputPath = new Path(args[2]);
		Path iacOutputPath = new Path(args[3]);
		
		Configuration confUser = new Configuration();
		confUser.setInt("index.fk.value", 0);
		Job jobUser = new Job(confUser, "Partition 1");
		jobUser.setJarByClass(SortAndPartition.class);
		jobUser.setMapperClass(UserIdMapper.class);
		
		FileInputFormat.setInputPaths(jobUser, userInputPath);
		FileOutputFormat.setOutputPath(jobUser, userOutputPath);
		
		jobUser.setMapOutputKeyClass(IntWritable.class);
		jobUser.setMapOutputValueClass(Text.class);

		jobUser.setPartitionerClass(UserIdPartitioner.class);
		UserIdPartitioner.init(jobUser, MAX_USER_ID);

		jobUser.setNumReduceTasks(MAX_USER_ID);

		int code = jobUser.waitForCompletion(true) ? 0 : 1;
		
		
		Configuration confIac = new Configuration();
		confIac.setInt("index.fk.value", 7);
		Job jobIac = new Job(confIac, "Partition 2");
		jobIac.setJarByClass(SortAndPartition.class);
		jobIac.setMapperClass(UserIdMapper.class);
		
		
		FileInputFormat.setInputPaths(jobIac, iacInputPath);
		FileOutputFormat.setOutputPath(jobIac, iacOutputPath);
		
		jobIac.setMapOutputKeyClass(IntWritable.class);
		jobIac.setMapOutputValueClass(Text.class);

		jobIac.setPartitionerClass(UserIdPartitioner.class);
		UserIdPartitioner.init(jobIac, MAX_USER_ID);

		jobIac.setNumReduceTasks(MAX_USER_ID);

		code += jobIac.waitForCompletion(true) ? 0 : 1;
		
		
		
		System.exit(code);

	}

}
