package com.aamend.hadoop.mapreduce.filter;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import com.aamend.hadoop.mapreduce.job.MRDPUtils;

public class BloomFilterMR {

	public static void main(String[] args) throws Exception {
		// Parse command line arguments
		Path inputFile = new Path(args[0]);
		int numMembers = Integer.parseInt(args[1]);
		float falsePosRate = Float.parseFloat(args[2]);
		Path bfFile = new Path(args[3]);
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
		for (FileStatus status : fs.listStatus(inputFile)) {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(fs.open(status.getPath()))));
			System.out.println("Reading " + status.getPath());
			while ((line = rdr.readLine()) != null) {
				filter.add(new Key(line.getBytes()));
				++numElements;
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
	}

	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

	public class BloomFilteringMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private BloomFilter filter = new BloomFilter();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Get file from the DistributedCache
			URI[] files = DistributedCache.getCacheFiles(context
					.getConfiguration());
			System.out.println("Reading Bloom filter from: "
					+ files[0].getPath());
			// Open local file for read.
			DataInputStream strm = new DataInputStream(new FileInputStream(
					files[0].getPath()));
			// Read into our Bloom filter.
			filter.readFields(strm);
			strm.close();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());
			// Get the value for the comment
			String comment = parsed.get("Text");
			StringTokenizer tokenizer = new StringTokenizer(comment);
			// For each word in the comment
			while (tokenizer.hasMoreTokens()) {
				// If the word is in the filter, output the record and break
				String word = tokenizer.nextToken();
				if (filter.membershipTest(new Key(word.getBytes()))) {
					context.write(value, NullWritable.get());
					break;
				}
			}
		}
	}
}
