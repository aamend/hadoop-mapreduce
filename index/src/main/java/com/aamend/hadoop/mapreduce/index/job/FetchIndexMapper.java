package com.aamend.hadoop.mapreduce.index.job;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FetchIndexMapper extends Mapper<Text, Text, Text, NullWritable> {

	private String indexLookup;
	private static final Logger LOGGER = Logger
			.getLogger(FetchIndexMapper.class);

	public void setup(Context context) {
		indexLookup = context.getConfiguration().get("target.lookup");
	}

	public void map(Text indexKey, Text indexValue, Context context)
			throws IOException, InterruptedException {

		int i = 0;
		String strKey = indexKey.toString();
		if (!strKey.equals(indexLookup)) {
			return;
		} else {
			for (String index : indexValue.toString().split(",")) {
				i++;
				LOGGER.debug("Found hash index " + index);
				context.write(new Text(index), NullWritable.get());
			}
		}
		LOGGER.info("Found " + i + " matching index(es)");
	}
}