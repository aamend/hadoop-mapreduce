package com.aamend.hadoop.mapreduce.index.job;

import java.io.IOException;

import com.aamend.hadoop.mapreduce.index.utils.CsvParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QueryMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private String targetValue;
	private int indexedColumn;
	private CsvParser parser;

	public void setup(Context context) {
		
		parser = new CsvParser();
		targetValue = context.getConfiguration().get("target.lookup");
		indexedColumn = context.getConfiguration().getInt("indexed.column", 0);
	}

	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {

		String targetToCompare = null;
		String[] csv = parser.parse(line.toString());
		try {
			targetToCompare = csv[indexedColumn];
		} catch (ArrayIndexOutOfBoundsException e) {
			return;
		}

		if (targetValue.equals(targetToCompare)) {
			context.write(line, NullWritable.get());
		} 
	}

}