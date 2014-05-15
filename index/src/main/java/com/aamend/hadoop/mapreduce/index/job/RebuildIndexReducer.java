package com.aamend.hadoop.mapreduce.index.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class RebuildIndexReducer extends Reducer<Text, Text, Text, Text> {

	private static final Logger LOGGER = Logger
			.getLogger(RebuildIndexReducer.class);

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> list = new ArrayList<String>();
		for (Text value : values) {
			String str = value.toString();
			if (!list.contains(str)) {
				list.add(str);
			}
		}

		int i = 0;
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String value : list) {
			i++;
			if (first) {
				sb.append(value);
				first = false;
			} else {
				sb.append(",");
				sb.append(value);
			}
		}
		context.write(key, new Text(sb.toString()));
		LOGGER.info("Successfuly indexed " + i + " values");
	}
}