package com.aamend.hadoop.mapreduce.index.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RebuildIndexCombiner extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> list = new ArrayList<String>();
		for (Text value : values) {
			String str = value.toString();
			if (!list.contains(str)) {
				list.add(str);
			}
		}
		for (String value : list) {
			context.write(key, new Text(value));
		}
	}
}
