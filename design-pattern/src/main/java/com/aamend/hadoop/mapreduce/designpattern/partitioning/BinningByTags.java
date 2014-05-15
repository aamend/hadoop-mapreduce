package com.aamend.hadoop.mapreduce.designpattern.partitioning;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class BinningByTags {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "binningByTag");

		job.setJarByClass(BinningByTags.class);

		// Set up input / output directories
		// Output directory must be set but will contain empty files
		Path inputPath = new Path(args[0]);

		FileInputFormat.setInputPaths(job, inputPath);
		// The name is essentially the output directory of the job
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, NullWritable.class);

		// Enable the counters for the job
		// If there are a significant number of different named outputs, this
		// should be disabled
		MultipleOutputs.setCountersEnabled(job, true);

		// Map-only job
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);

	}

	public class BinningByTagsMapper extends
			Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;

		protected void setup(Context context) {
			// Create a new MultipleOutputs using the context object
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

		public void map(Object text, Text value, Context context)
				throws IOException, InterruptedException {

			Map<String, String> parsed = MRDPUtils.xmlToMap(value.toString());

			// Get the body of the post
			String post = parsed.get("Body");
			// If the post contains the word "hadoop", write it to its own bin
			if (post.toLowerCase().contains("hadoop")) {
				mos.write("bins", value, NullWritable.get(), "hadoop-post");
			}

			String rawtags = parsed.get("Tags");
			// Tags are delimited by ><. i.e. <tag1><tag2><tag3>
			String[] tagTokens = StringEscapeUtils.unescapeHtml(rawtags).split(
					"><");
			for (String tag : tagTokens) {

				tag = tag.replaceAll(">|<", "").toLowerCase();
				if (tag.equalsIgnoreCase("hadoop")) {
					mos.write("bins", value, NullWritable.get(), "hadoop-tag");
				}
				if (tag.equalsIgnoreCase("hive")) {
					mos.write("bins", value, NullWritable.get(), "hive-tag");
				}
				if (tag.equalsIgnoreCase("pig")) {
					mos.write("bins", value, NullWritable.get(), "pig-tag");
				}
				if (tag.equalsIgnoreCase("hbase")) {
					mos.write("bins", value, NullWritable.get(), "hbase-tag");
				}

			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Close multiple outputs otherwise you will not get any values
			mos.close();
		}

	}

}
