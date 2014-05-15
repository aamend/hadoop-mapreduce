package com.aamend.hadoop.mapreduce.index.job;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {

	public void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		context.write(key, NullWritable.get());
	}

}
