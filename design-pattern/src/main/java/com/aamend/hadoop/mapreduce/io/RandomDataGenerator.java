package com.aamend.hadoop.mapreduce.io;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RandomDataGenerator {

	public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
	public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
	public static final String RANDOM_WORD_LIST = "random.generator.random.word.file";

	public static void main(String[] args) throws Exception {

		// Get CLI parameters
		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
		Path wordList = new Path(args[2]);
		Path outputDir = new Path(args[3]);

		// Create job
		Configuration conf = new Configuration();
		Job job = new Job(conf, "RandomDataGenerationDriver");
		job.setJarByClass(RandomDataGenerator.class);

		// Setup mapreduce (identity mapper only)
		job.setNumReduceTasks(0);

		// Input
		job.setInputFormatClass(RandomStackOverflowInputFormat.class);
		RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
		RandomStackOverflowInputFormat.setNumRecordPerTask(job,
				numRecordsPerTask);
		RandomStackOverflowInputFormat.setRandomWordList(job, wordList);

		// Output
		TextOutputFormat.setOutputPath(job, outputDir);

		// Specify key / Value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

	public static class RandomStackOverflowInputFormat extends
			InputFormat<Text, NullWritable> {

		public static void setNumMapTasks(Job job, int numMapTasks) {
			job.getConfiguration().setInt(NUM_MAP_TASKS, numMapTasks);
		}

		public static void setNumRecordPerTask(Job job, int numRecordsPerTask) {
			job.getConfiguration().setInt(NUM_RECORDS_PER_TASK,
					numRecordsPerTask);
		}

		public static void setRandomWordList(Job job, Path wordList)
				throws IOException {
			DistributedCache.addCacheFile(wordList.toUri(),
					job.getConfiguration());
		}

		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException,
				InterruptedException {

			// Other way around, give as much FakeInputSplit as you need map
			int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
			ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
			for (int i = 0; i < numSplits; i++) {
				splits.add(new FakeInputSplit());
			}
			return splits;
		}

		@Override
		public RecordReader<Text, NullWritable> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			// Create a new RandomStackOverflowRecordReader and initialize it
			RandomStackOverflowRecordReader rr = new RandomStackOverflowRecordReader();
			rr.initialize(split, context);
			return rr;
		}

	}

	public static class RandomStackOverflowRecordReader extends
			RecordReader<Text, NullWritable> {

		private int numRecordsToCreate = 0;
		private int createdRecords = 0;
		private Text key = new Text();
		private NullWritable value = NullWritable.get();
		private Random rndm = new Random();
		private ArrayList<String> randomWords = new ArrayList<String>();
		// This object will format the creation date string into a Date
		// object
		private SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			// Get the number of records to create from the configuration
			this.numRecordsToCreate = context.getConfiguration().getInt(
					NUM_RECORDS_PER_TASK, -1);

			// Get the list of random words from the DistributedCache
			URI[] files = DistributedCache.getCacheFiles(context
					.getConfiguration());

			// Read the list of random words into a list
			BufferedReader rdr = new BufferedReader(new FileReader(
					files[0].toString()));
			String line;
			while ((line = rdr.readLine()) != null) {
				randomWords.add(line);
			}
			rdr.close();

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			// If we still have records to create
			if (createdRecords < numRecordsToCreate) {
				// Generate random data
				int score = Math.abs(rndm.nextInt()) % 15000;
				int rowId = Math.abs(rndm.nextInt()) % 1000000000;
				int postId = Math.abs(rndm.nextInt()) % 100000000;
				int userId = Math.abs(rndm.nextInt()) % 1000000;
				String creationDate = frmt.format(Math.abs(rndm.nextLong()));
				// Create a string of text from the random words
				String text = getRandomText();
				String randomRecord = "<row Id=\"" + rowId + "\" PostId=\""
						+ postId + "\" Score=\"" + score + "\" Text=\"" + text
						+ "\" CreationDate=\"" + creationDate + "\" UserId\"="
						+ userId + "\" />";
				key.set(randomRecord);
				++createdRecords;
				return true;
			} else {
				// We are done creating records
				return false;
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float) createdRecords / (float) numRecordsToCreate;
		}

		@Override
		public void close() throws IOException {
			// nothing to do here...

		}

		private String getRandomText() {
			StringBuilder bldr = new StringBuilder();
			int numWords = Math.abs(rndm.nextInt()) % 30 + 1;
			for (int i = 0; i < numWords; ++i) {
				bldr.append(randomWords.get(Math.abs(rndm.nextInt())
						% randomWords.size())
						+ " ");
			}
			return bldr.toString();
		}

	}

	public static class FakeInputSplit extends InputSplit implements Writable {

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// Useless here
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// Useless here
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			// Fake location
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			// Fake locations
			return new String[0];
		}

	}

}
