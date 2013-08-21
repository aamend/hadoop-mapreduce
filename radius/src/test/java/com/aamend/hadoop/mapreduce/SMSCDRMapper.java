package com.aamend.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text smsStatus = new Text();
	private final static IntWritable ONE = new IntWritable(1);

	static enum CDRCounter {
		NonSMSCDR;
	};

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		String[] line = value.toString().split(";");
		if (Integer.parseInt(line[1]) == 1) {
			smsStatus.set(line[4]);
			context.write(smsStatus, ONE);
		} else {
			context.getCounter(CDRCounter.NonSMSCDR).increment(1);
		}
	}
}