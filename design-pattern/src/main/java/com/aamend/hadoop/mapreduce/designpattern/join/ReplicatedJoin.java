package com.aamend.hadoop.mapreduce.designpattern.join;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class ReplicatedJoin {

	private static Logger LOGGER = LoggerFactory
			.getLogger(ReplicatedJoin.class);

	/*
	 * A replicated join is a special type of join operation between one large
	 * and many small data sets that can be performed on the map-side. This
	 * pattern completely eliminates the need to shuffle any data to the reduce
	 * phase.
	 */

	/*
	 * A replicated join is an extremely useful, but has a strict size limit on
	 * all but one of the data sets to be joined. All the data sets except the
	 * very large one are essentially read into memory during the setup phase of
	 * each map task, which is limited by the JVM heap. If you can live within
	 * this limitation, you get a drastic benefit because there is no reduce
	 * phase at all, and therefore no shuffling or sorting. The join is done
	 * entirely in the map phase, with the very large data set being the input
	 * for the MapReduce job.
	 */

	/*
	 * There is an additional restriction that a replicated join is really
	 * useful only for an inner or a left outer join where the large data set is
	 * the “left” data set. The other join types require a reduce phase to group
	 * the “right” data set with the entirety of the left data set. Although
	 * there may not be a match for the data stored in memory for a given map
	 * task, there could be match in another input split. Because of this, we
	 * will restrict this pattern to inner and left outer joins.
	 */

	/*
	 * The type of join to execute is an inner join or a left outer join, with
	 * the large input data set being the “left” part of the operation. All of
	 * the data sets, except for the large one, can be fit into main memory of
	 * each map task.
	 */

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		// Small dataset to Join
		FileSystem hdfs = FileSystem.get(conf);
		LOGGER.info("Find files in path " + args[0]);
		RemoteIterator<LocatedFileStatus> ri = hdfs.listFiles(
				new Path(args[0]), true);
		boolean atLeastOne = false;
		while (ri.hasNext()) {
			LocatedFileStatus lfs = ri.next();
			Path file = lfs.getPath();
			LOGGER.info("Adding file " + file.toString()
					+ " to distributed cache");
			DistributedCache.addFileToClassPath(file, conf);
			atLeastOne = true;
		}

		if (!atLeastOne) {
			String msg = "Was not able to add any file to distributed cache";
			LOGGER.error(msg);
			throw new IOException(msg);
		}

		// Job must be created AFTER you add files to cache
		Job job = new Job(conf, "replicatedJoin");

		// Full dataset to browse
		TextInputFormat.addInputPath(job, new Path(args[1]));

		job.setJarByClass(ReplicatedJoin.class);
		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setReducerClass(ReplicatedJoinReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.getConfiguration().set("join.type", args[3]);

		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

	public class ReplicatedJoinMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		HashMap<String, String> inMemoryUsers;
		String joinType;

		public void setup(Context context) throws IOException {

			inMemoryUsers = new HashMap<String, String>();
			Path[] files = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());

			joinType = context.getConfiguration().get("join.type");
			Pattern pattern = Pattern.compile("^(\\d+)\\,(.*)$");

			if (files == null) {
				String msg = "Could not find files in distributed cache";
				LOGGER.error(msg);
				throw new IOException(msg);
			}

			LOGGER.info("will now read in memory " + files.length + " files");
			for (Path file : files) {
				System.out.println("Will now read in memory file " + file);
				Scanner scan = new Scanner(new File(file.toString()));
				while (scan.hasNext()) {
					String line = scan.nextLine();
					Matcher m1 = pattern.matcher(line);
					while (m1.find()) {
						String userId = m1.group(1);
						String userInfo = m1.group(2);
						inMemoryUsers.put(userId, userId + "," + userInfo);
					}
				}
			}

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] values = MRDPUtils.csvToStringArray(value.toString());
			String userId = values[7];
			String joinValue = inMemoryUsers.get(userId);
			Text output = new Text();

			if (joinType.equalsIgnoreCase("inner")) {
				// Only if user is known
				if (joinValue != null && !joinValue.isEmpty()) {
					output.set(value.toString() + "," + joinValue);
				}
			} else if (joinType.equalsIgnoreCase("leftouter")) {
				// Even if user is not known
				if (joinValue == null || joinValue.isEmpty()) {
					output.set(value.toString());
				} else {
					output.set(value.toString() + "," + joinValue);
				}
			}
			context.write(output, NullWritable.get());
		}

	}

	public class ReplicatedJoinReducer extends
			Reducer<Text, Iterable<NullWritable>, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

}
