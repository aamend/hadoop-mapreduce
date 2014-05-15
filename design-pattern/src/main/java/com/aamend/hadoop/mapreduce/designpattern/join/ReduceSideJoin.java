package com.aamend.hadoop.mapreduce.designpattern.join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class ReduceSideJoin {

	public static Text EMPTY_TEXT = new Text("");

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceSideJoin.class);

		// First dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, UserMapper.class);

		// Second dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, IpMapper.class);

		job.setReducerClass(UserIpReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().set("join.type", args[3]);
		job.getConfiguration().set("target.ip", args[4]);

		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}
	
	public class IpMapper extends Mapper<LongWritable, Text, Text, Text> {

		public String target;

		public void setup(Context context) {
			target = context.getConfiguration().get("target.ip");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] csv = MRDPUtils.csvToStringArray(value.toString());
			String userId = csv[7];
			String ip = csv[2];
			if (target.equals(ip)) {
				context.write(new Text(userId), new Text("B" + value.toString()));
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

	public class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] csv = MRDPUtils.csvToStringArray(value.toString());
			String userId = csv[0];
			context.write(new Text(userId), new Text("A" + value.toString()));
		}
	}

}
