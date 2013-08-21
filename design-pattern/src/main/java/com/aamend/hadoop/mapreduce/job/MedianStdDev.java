package com.aamend.hadoop.mapreduce.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDev {

	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");
	private static final LongWritable ONE = new LongWritable(1);

	public class MedianStdDevMapper extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		private IntWritable outHour = new IntWritable();
		private IntWritable outCommentLength = new IntWritable();

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
				// set the comment length
				outCommentLength.set(text.length());
				// write out the user ID with min max dates and count
				context.write(outHour, outCommentLength);
			} catch (ParseException e) {
				System.err.println("DATE Error " + e.getMessage());
			}

		}
	}

	public class MedianStdDevReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private ArrayList<Float> commentLengths = new ArrayList<Float>();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			float sum = 0;
			float count = 0;
			commentLengths.clear();
			result.setStdDev(0);

			// Iterate through all input values for this key
			for (IntWritable val : values) {
				commentLengths.add((float) val.get());
				sum += val.get();
				++count;
			}

			// sort commentLengths to calculate median
			Collections.sort(commentLengths);

			// if commentLengths is an even value, average middle two elements
			if (count % 2 == 0) {
				result.setMedian((commentLengths.get((int) count / 2 - 1) + commentLengths
						.get((int) count / 2)) / 2.0f);
			} else {
				// else, set median to middle value
				result.setMedian(commentLengths.get((int) count / 2));
			}

			// calculate standard deviation
			float mean = sum / count;
			float sumOfSquares = 0.0f;
			for (Float f : commentLengths) {
				sumOfSquares += (f - mean) * (f - mean);
			}
			result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));
			context.write(key, result);
		}
	}

	public class MedianStdDevMapperWithCombiner extends
			Mapper<Object, Text, IntWritable, SortedMapWritable> {
		private IntWritable commentLength = new IntWritable();

		private IntWritable outHour = new IntWritable();

		@SuppressWarnings("deprecation")
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");
			// Grab the comment to find the length
			String text = parsed.get("Text");
			// Get the hour this comment was posted in
			Date creationDate;
			try {
				creationDate = frmt.parse(strDate);
				outHour.set(creationDate.getHours());
				commentLength.set(text.length());
				SortedMapWritable outCommentLength = new SortedMapWritable();
				outCommentLength.put(commentLength, ONE);
				// Write out the user ID with min max dates and count
				context.write(outHour, outCommentLength);
			} catch (ParseException e) {
				System.err.println("ERROR on date " + e.getMessage());
			}

		}
	}

	public class MedianStdDevReducerWithCombiner
			extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<Integer, Long>();

		public void reduce(IntWritable key, Iterable<SortedMapWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			long totalComments = 0;
			commentLengthCounts.clear();
			result.setMedian(0);
			result.setStdDev(0);

			// Write or consolidate TreeMap
			for (SortedMapWritable v : values) {
				for (@SuppressWarnings("rawtypes")
				Entry<WritableComparable, Writable> entry : v.entrySet()) {
					int length = ((IntWritable) entry.getKey()).get();
					long count = ((LongWritable) entry.getValue()).get();
					totalComments += count;
					sum += length * count;
					Long storedCount = commentLengthCounts.get(length);
					if (storedCount == null) {
						commentLengthCounts.put(length, count);
					} else {
						commentLengthCounts.put(length, storedCount + count);
					}
				}
			}

			long medianIndex = totalComments / 2L;
			long previousComments = 0;
			long comments = 0;
			int prevKey = 0;

			// Calculate median
			for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
				comments = previousComments + entry.getValue();

				if (previousComments <= medianIndex && medianIndex < comments) {
					if (totalComments % 2 == 0
							&& previousComments == medianIndex) {
						result.setMedian((float) (entry.getKey() + prevKey) / 2.0f);
					} else {
						result.setMedian(entry.getKey());
					}
					break;
				}

				previousComments = comments;
				prevKey = entry.getKey();
			}

			// calculate standard deviation
			float mean = sum / totalComments;
			float sumOfSquares = 0.0f;
			for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
				sumOfSquares += (entry.getKey() - mean)
						* (entry.getKey() - mean) * entry.getValue();
			}
			result.setStdDev((float) Math.sqrt(sumOfSquares
					/ (totalComments - 1)));
			context.write(key, result);
		}
	}

	public class MedianStdDevCombiner
			extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {

		SortedMapWritable outValue = new SortedMapWritable();

		protected void reduce(IntWritable key,
				Iterable<SortedMapWritable> values, Context context)
				throws IOException, InterruptedException {
			for (SortedMapWritable v : values) {
				for (@SuppressWarnings("rawtypes")
				Entry<WritableComparable, Writable> entry : v.entrySet()) {
					LongWritable count = (LongWritable) outValue.get(entry
							.getKey());
					if (count != null) {
						count.set(count.get()
								+ ((LongWritable) entry.getValue()).get());
					} else {
						outValue.put(entry.getKey(), new LongWritable(
								((LongWritable) entry.getValue()).get()));
					}
				}
			}
			context.write(key, outValue);
		}
	}

	public class MedianStdDevTuple implements Writable {

		private float median;
		private float stdDev;

		public float getMedian() {
			return median;
		}

		public void setMedian(float median) {
			this.median = median;
		}

		public float getStdDev() {
			return stdDev;
		}

		public void setStdDev(float stdDev) {
			this.stdDev = stdDev;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(median);
			out.writeFloat(stdDev);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			median = in.readFloat();
			stdDev = in.readFloat();
		}

		public String toString() {
			return median + "\t" + stdDev;
		}

	}

}
