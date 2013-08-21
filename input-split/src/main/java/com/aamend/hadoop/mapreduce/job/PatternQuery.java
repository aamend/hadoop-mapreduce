package com.aamend.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aamend.hadoop.mapreduce.format.FileFilter;
import com.aamend.hadoop.mapreduce.job.PatternQueryMapper;
import com.aamend.hadoop.mapreduce.job.PatternQueryReducer;

public class PatternQuery extends Configured implements Tool {

	private Path inputPath;
	private Path outputPath;

	public int run(String[] args) throws Exception {

		// When implementing tool
		Configuration conf = this.getConf();

		System.out.println("File mtime = " + conf.get("file.mtime"));

		if (conf.get("file.pattern") == null) {
			conf.set("file.pattern", ".*");
		}

		if (conf.get("target.pattern") == null) {
			conf.set("target.pattern", ".*");
		}

		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);

		// Delete output path if exist
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		// ------------------------------------------
		// Create job
		// ------------------------------------------
		Job jobQuery = new Job(conf, "PatternQuery");
		jobQuery.setJarByClass(PatternQuery.class);

		// Setup MapReduce job
		jobQuery.setMapperClass(PatternQueryMapper.class);
		jobQuery.setReducerClass(PatternQueryReducer.class);

		// Specify key / value
		jobQuery.setOutputKeyClass(Text.class);
		jobQuery.setOutputValueClass(NullWritable.class);

		// Input
		FileInputFormat.setInputPathFilter(jobQuery, FileFilter.class);
		FileInputFormat.addInputPath(jobQuery, inputPath);
		jobQuery.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(jobQuery, outputPath);
		jobQuery.setOutputFormatClass(TextOutputFormat.class);

		// Execute job and return status
		return jobQuery.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PatternQuery(), args);
		System.exit(res);
	}

}