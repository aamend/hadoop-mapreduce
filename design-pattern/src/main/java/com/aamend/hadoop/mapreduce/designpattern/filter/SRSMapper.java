package com.aamend.hadoop.mapreduce.designpattern.filter;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//simple random sampling

public class SRSMapper extends Mapper<Object, Text, NullWritable, Text> {

	private Random rands = new Random();
	private Double percentage;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		// Retrieve the percentage that is passed in via the configuration
		// like this: conf.set("filter_percentage", .5);
		// for .5%
		String strPercentage = context.getConfiguration().get(
				"filter_percentage");
		percentage = Double.parseDouble(strPercentage) / 100.0;
	}
	
	//If this is the case, set the number of reducers to 1 without
	//specifying a reducer class, which will tell the MapReduce framework to use a single
	//identity reducer that simply collects the output into a single file.

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if (rands.nextDouble() < percentage) {
			context.write(NullWritable.get(), value);
		}
	}
}
