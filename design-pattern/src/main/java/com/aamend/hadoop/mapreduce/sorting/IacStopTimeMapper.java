package com.aamend.hadoop.mapreduce.sorting;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aamend.hadoop.mapreduce.job.MRDPUtils;

public class IacStopTimeMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] csv = MRDPUtils.csvToStringArray(value.toString());
		Text date = new Text(csv[0]);
		context.write(date, value);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

}
