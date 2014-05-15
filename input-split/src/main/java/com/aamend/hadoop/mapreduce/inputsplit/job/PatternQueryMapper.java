package com.aamend.hadoop.mapreduce.inputsplit.job;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PatternQueryMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	private Pattern targetPattern;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String csvLine = value.toString();
		Matcher m1 = targetPattern.matcher(csvLine);
		if (m1.matches()) {
			context.write(value, NullWritable.get());
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		targetPattern = Pattern.compile(conf.get("target.pattern"));
	}

}
