package com.aamend.hadoop.mapreduce.designpattern.join;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserIdPartitioner extends Partitioner<IntWritable, Text> implements
		Configurable {

	private int maxUserId;
	private Configuration conf;
	private static String MAX_USER_ID = "max.user.id";

	public static void init(Job job, int maxUserId) {
		job.getConfiguration().setInt(MAX_USER_ID, maxUserId);
	}

	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() % maxUserId;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		maxUserId = conf.getInt(MAX_USER_ID, 0);
	}

}
