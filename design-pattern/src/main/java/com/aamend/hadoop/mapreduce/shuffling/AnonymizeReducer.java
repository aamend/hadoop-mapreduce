package com.aamend.hadoop.mapreduce.shuffling;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnonymizeReducer extends
		Reducer<FloatWritable, Text, Text, NullWritable> {

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void reduce(FloatWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text value : values){
			context.write(value, NullWritable.get());
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

}
