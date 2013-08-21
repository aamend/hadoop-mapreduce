package com.aamend.hadoop.mapreduce.filter;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.aamend.hadoop.mapreduce.job.MRDPUtils;

public class TopTen {

	public class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			String reputation = parsed.get("Reputation");
			// Add this record to our map with the reputation as the key
			repToRecordMap.put(Integer.parseInt(reputation), new Text(value));
			// If we have more than ten records, remove the one with the lowest
			// rep
			// As this tree map is sorted in descending order, the user with
			// the lowest reputation is the last key.
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		// Good to know, while setup is called at the early beginnng, cleanup at
		// really end
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Output our ten records to the reducers with a null key
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public class TopTenReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		// Overloads the comparator to order the reputations in descending order
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				Map<String, String> parsed = MRDPUtils.xmlToMap(value
						.toString());
				repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")),
						new Text(value));
				// If we have more than ten records, remove the one with the
				// lowest
				// rep
				// As this tree map is sorted in descending order, the user with
				// the lowest reputation is the last key.
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			for (Text t : repToRecordMap.descendingMap().values()) {
				// Output our ten records to the file system with a null key
				context.write(NullWritable.get(), t);
			}
		}
	}
}
