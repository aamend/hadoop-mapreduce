package com.aamend.hadoop.mapreduce.designpattern.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class UserIdMapper extends Mapper<Object, Text, IntWritable, Text> {

	private int index;

	public void setup(Context context) {
		index = context.getConfiguration().getInt("index.fk.value", 0);
	}

	public void map(Object text, Text value, Context context)
			throws IOException, InterruptedException {

		IntWritable userId = new IntWritable();
		String[] csv = MRDPUtils.csvToStringArray(value.toString());
		userId.set(Integer.parseInt(csv[index]));
		context.write(userId, value);
	}

}