package com.aamend.hadoop.mapreduce.designpattern.filter;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class DistinctUser {

	public class DistinctUserMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private Text outUserId = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// Get the value for the UserId attribute
			String userId = parsed.get("UserId");
			// Set our output key to the user's id
			outUserId.set(userId);
			// Write the user's id with a null value
			context.write(outUserId, NullWritable.get());
		}
	}

	public class DistinctUserReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			// Write the user's id with a null value
			context.write(key, NullWritable.get());
		}
	}
}
