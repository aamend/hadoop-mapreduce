package com.aamend.hadoop.mapreduce.job;

import java.io.IOException;

import com.aamend.hadoop.mapreduce.utils.CsvParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aamend.hadoop.mapreduce.utils.Utils;

public class RebuildIndexMapper extends Mapper<Object, Text, Text, Text> {

	private String splitId;
	private int indexedColumn;
	private CsvParser parser;
	
	public void setup(Context context) {

		parser = new CsvParser();
		Configuration conf = context.getConfiguration();
		indexedColumn = conf.getInt("indexed.column", 0);
		splitId = Utils.inputSplitToString(context.getInputSplit());
	}

	public void map(Object object, Text line, Context context)
			throws IOException, InterruptedException {

		String key = null;
		
		String[] csv = parser.parse(line.toString());
		try {
			key = csv[indexedColumn];
		} catch (ArrayIndexOutOfBoundsException e) {
			return;
		}

		if (key != null && !key.isEmpty()) {
			Text value = new Text(splitId);
			context.write(new Text(key), value);
		}

	}
}
