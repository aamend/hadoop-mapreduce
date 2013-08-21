package com.aamend.hadoop.mapreduce.join;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import com.aamend.hadoop.mapreduce.job.MRDPUtils;

public class ReduceSideJoinBloomFilter {

	public static Text EMPTY_TEXT = new Text("");

	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		// Parse command line arguments
		Path userFile = new Path(args[0]);
		Path iacFile = new Path(args[1]);
		Path bfFile = new Path(args[2]);
		Path outputFile = new Path(args[3]);
		int maxBirthDate = Integer.parseInt(args[4]);
		String joinType = args[5];

		int numMembers = Integer.parseInt(args[6]);
		float falsePosRate = Float.parseFloat(args[7]);

		// Calculate our vector size and optimal K value based on
		// approximations
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = getOptimalK(numMembers, vectorSize);
		// Create new Bloom filter
		BloomFilter filter = new BloomFilter(vectorSize, nbHash,
				Hash.MURMUR_HASH);
		System.out.println("Training Bloom filter of size " + vectorSize
				+ " with " + nbHash + " hash functions, " + numMembers
				+ " approximate number of records, and " + falsePosRate
				+ " false positive rate");

		// Open file for read
		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());
		for (FileStatus status : fs.listStatus(userFile)) {

			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					new FSDataInputStream(fs.open(status.getPath()))));
			System.out.println("Reading " + status.getPath());
			while ((line = rdr.readLine()) != null) {
				String[] csv = MRDPUtils.csvToStringArray(line);
				String birth = csv[2];
				String id = csv[0];
				int year = Integer.parseInt(birth.split("-")[0]);
				if (year < maxBirthDate) {
					System.out.println(year + "<" + maxBirthDate);
					filter.add(new Key(id.getBytes()));
					++numElements;
				}
			}
			rdr.close();
		}

		System.out.println("Trained Bloom filter with " + numElements
				+ " entries.");
		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();

		Configuration conf = new Configuration();
		DistributedCache.addFileToClassPath(bfFile, conf);
		conf.setInt("max.birth", maxBirthDate);
		Job job = new Job(conf, "ReduceSideJoinBloomFilter");
		job.setJarByClass(ReduceSideJoinBloomFilter.class);

		// First dataset to Join
		MultipleInputs.addInputPath(job, userFile, TextInputFormat.class,
				UserMapper.class);

		// Second dataset to Join
		MultipleInputs.addInputPath(job, iacFile, TextInputFormat.class,
				IpMapperWithBloom.class);

		job.setReducerClass(UserIpReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().set("join.type", joinType);

		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

	public class IpMapperWithBloom extends
			Mapper<LongWritable, Text, Text, Text> {

		private BloomFilter bfilter = new BloomFilter();

		// Retrieve bloom filter from Distributed cache
		public void setup(Context context) throws IOException {
			Path[] files = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			DataInputStream strm = new DataInputStream(new FileInputStream(
					new File(files[0].toString())));
			bfilter.readFields(strm);
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] csv = MRDPUtils.csvToStringArray(value.toString());
			String userId = csv[7];

			if (bfilter.membershipTest(new Key(userId.getBytes()))) {
				context.write(new Text(userId),
						new Text("B" + value.toString()));
			}
		}
	}

	public class UserIpReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		public String joinType;

		public void setup(Context context) {
			joinType = context.getConfiguration().get("join.type");
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Clear our lists
			listA.clear();
			listB.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with. Make sure to remove the tag!
			for (Text value : values) {
				if (value.charAt(0) == 'A') {
					listA.add(new Text(value.toString().substring(1)));
				} else if (value.charAt(0) == 'B') {
					listB.add(new Text(value.toString().substring(1)));
				}
			}

			// Execute our join logic now that the lists are filled
			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException,
				InterruptedException {

			if (joinType.equalsIgnoreCase("inner")) {
				// If both lists are not empty, join A with B
				if (!listA.isEmpty() && !listB.isEmpty()) {
					for (Text A : listA) {
						for (Text B : listB) {
							context.write(A, B);
						}
					}
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// For each entry in A,
				for (Text A : listA) {
					// If list B is not empty, join A and B
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							context.write(A, B);
						}
					} else {
						// Else, output A by itself
						context.write(A, EMPTY_TEXT);
					}
				}
			} else if (joinType.equalsIgnoreCase("rightouter")) {
				// For each entry in B,
				for (Text B : listB) {
					// If list A is not empty, join A and B
					if (!listA.isEmpty()) {
						for (Text A : listA) {
							context.write(A, B);
						}
					} else {
						// Else, output B by itself
						context.write(EMPTY_TEXT, B);
					}
				}
			} else if (joinType.equalsIgnoreCase("fullouter")) {
				// If list A is not empty
				if (!listA.isEmpty()) {
					// For each entry in A
					for (Text A : listA) {
						// If list B is not empty, join A with B
						if (!listB.isEmpty()) {
							for (Text B : listB) {
								context.write(A, B);
							}
						} else {
							// Else, output A by itself
							context.write(A, EMPTY_TEXT);
						}
					}
				} else {
					// If list A is empty, just output B
					for (Text B : listB) {
						context.write(EMPTY_TEXT, B);
					}
				}
			} else if (joinType.equalsIgnoreCase("anti")) {
				// If list A is empty and B is empty or vice versa
				if (listA.isEmpty() ^ listB.isEmpty()) {
					// Iterate both A and B with null values
					// The previous XOR check will make sure exactly one of
					// these lists is empty and therefore the list will be
					// skipped
					for (Text A : listA) {
						context.write(A, EMPTY_TEXT);
					}
					for (Text B : listB) {
						context.write(EMPTY_TEXT, B);
					}
				}
			}
		}
	}

	// Get only user born before...
	public class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

		public int maxBirth;

		public void setup(Context context) {
			maxBirth = context.getConfiguration().getInt("max.birth", 1980);
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] csv = MRDPUtils.csvToStringArray(value.toString());
			String userId = csv[0];
			String[] birthDate = csv[2].split("-");
			int year = Integer.parseInt(birthDate[0]);
			if (year < maxBirth) {
				context.write(new Text(userId),
						new Text("A" + value.toString()));
			}
		}
	}

}
