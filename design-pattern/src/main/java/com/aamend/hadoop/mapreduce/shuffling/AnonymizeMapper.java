package com.aamend.hadoop.mapreduce.shuffling;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aamend.hadoop.mapreduce.job.MRDPUtils;

public class AnonymizeMapper extends Mapper<Object, Text, FloatWritable, Text> {

	private int ipIndex = 2;
	private int msisdnIndex = 3;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		Random rand = new Random();
		float f = rand.nextFloat();

		StringBuilder sb = new StringBuilder();
		String[] csv = MRDPUtils.csvToStringArray(value.toString());
		for (int i = 0; i < csv.length; i++) {

			if (i != ipIndex && i != msisdnIndex) {
				if (i != 0) {
					sb.append(",");
				}
				sb.append(csv[i]);
			}
		}

		Text output = new Text(sb.toString());
		context.write(new FloatWritable(f), output);
	}

}
