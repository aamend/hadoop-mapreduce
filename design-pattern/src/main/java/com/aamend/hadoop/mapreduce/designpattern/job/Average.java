package com.aamend.hadoop.mapreduce.designpattern.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Average {

	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");

	public class AverageMapper extends
			Mapper<Object, Text, IntWritable, CountAverageTuple> {
		private IntWritable outHour = new IntWritable();
		private CountAverageTuple coutCountAverage = new CountAverageTuple();

		@SuppressWarnings("deprecation")
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");
			// Grab the comment to find the length
			String text = parsed.get("Text");
			// get the hour this comment was posted in
			Date creationDate;
			try {
				creationDate = frmt.parse(strDate);
				outHour.set(creationDate.getHours());
				// get the comment length
				coutCountAverage.setCount(1);
				coutCountAverage.setAverage(text.length());
				// write out the hour with the comment length
				context.write(outHour, coutCountAverage);
			} catch (ParseException e) {
				System.err.println("Date error " + e.getMessage());
			}

		}
	}

	public class AverageReducer
			extends
			Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {

		private CountAverageTuple result = new CountAverageTuple();

		public void reduce(IntWritable key, Iterable<CountAverageTuple> values,
				Context context) throws IOException, InterruptedException {
			/*
			 * When determining an average, the reducer code can be used as a
			 * combiner when outputting the count along with the average. An
			 * average is not an associative operation, but if the count is
			 * output from the reducer with the count, these two values can be
			 * multiplied to preserve the sum for the final reduce phase
			 * 
			 * The only differentiation between the two classes is that the
			 * reducer does not write the count with the average.
			 */
			float count = 0;
			float sum = 0;
			for (CountAverageTuple value : values) {
				count += value.getCount();
				sum += value.getCount() * value.getAverage();
			}

			result.setCount(count);
			result.setAverage(sum / count);

			context.write(key, result);

		}

	}

	public class CountAverageTuple implements Writable {

		private float count;
		private float average;

		public float getCount() {
			return count;
		}

		public void setCount(float count) {
			this.count = count;
		}

		public float getAverage() {
			return average;
		}

		public void setAverage(float average) {
			this.average = average;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(count);
			out.writeFloat(average);

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readFloat();
			average = in.readFloat();
		}

		public String toString() {
			return count + "\t" + average;
		}

	}

}
