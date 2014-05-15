package com.aamend.hadoop.mapreduce.index.job;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FetchIndexReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {

	public void reduce(Text index, Iterable<NullWritable> useless,
			Context context) throws IOException, InterruptedException {
		context.write(index, NullWritable.get());
	}

}
