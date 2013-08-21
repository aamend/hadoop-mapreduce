package com.aamend.hadoop.mapreduce.service;

import java.io.File;
import java.util.HashMap;

import com.aamend.hadoop.mapreduce.job.*;
import com.aamend.hadoop.mapreduce.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.aamend.hadoop.mapreduce.index.IndexedQuery;
import com.aamend.hadoop.mapreduce.index.Query;
import com.aamend.hadoop.mapreduce.index.RebuildIndex;
import com.aamend.hadoop.mapreduce.fileInput.IndexFileInputFormat;

public class IndexService {

	private static final Logger LOGGER = Logger.getLogger(IndexService.class);

	/**
	 * Get record for a given target based on existing index
	 * 
	 * @param indexColumn
	 * @param strInputPath
	 * @param strOutputPath
	 * @param strOutputIndexPath
	 * @param targetValue
	 * @return Metadata
	 * @throws Exception
	 */
	public HashMap<String, String> queryWithIndex(int indexColumn,
			String strInputPath, String strOutputPath,
			String strOutputIndexPath, String targetValue) throws Exception {

		String jobName = "queryWithIndex";
		LOGGER.info("Will now execute " + jobName + " job");

		Configuration conf = new Configuration();
		conf.set("target.lookup", targetValue);
		conf.set("index.output.path", strOutputIndexPath + File.separator
				+ targetValue);
		conf.setInt("indexed.column", indexColumn);
		conf.set("mapred.job.priority", "VERY_LOW");

		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputPath + File.separator + targetValue);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		Job job = new Job(conf, jobName);
		job.setJarByClass(IndexedQuery.class);
		job.setMapperClass(QueryMapper.class);
		job.setReducerClass(QueryReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(IndexFileInputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(false)) {
			LOGGER.error("Test " + jobName + " returned error");
			System.exit(1);
		} else {
			LOGGER.info("Test " + jobName + " successful");
		}
		return Utils.printCounters(job.getCounters());
	}

	/**
	 * Query full index table for a particular target
	 * 
	 * @param indexColumn
	 * @param strInputPath
	 * @param strOutputIndexPath
	 * @param targetValue
	 * @return Metadata
	 * @throws Exception
	 */
	public HashMap<String, String> fetchIndex(int indexColumn,
			String strInputPath, String strOutputIndexPath, String targetValue)
			throws Exception {

		String jobName = "fetchIndex";
		LOGGER.info("Will now execute " + jobName + " job");

		Configuration conf = new Configuration();
		conf.set("target.lookup", targetValue);
		conf.set("index.output.path", strOutputIndexPath + File.separator
				+ targetValue);
		conf.setInt("indexed.column", indexColumn);
		conf.set("mapred.job.priority", "VERY_LOW");

		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputIndexPath + File.separator
				+ targetValue);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		Job job = new Job(conf, jobName);
		job.setJarByClass(IndexedQuery.class);
		job.setMapperClass(FetchIndexMapper.class);
		job.setReducerClass(FetchIndexReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(false)) {
			LOGGER.error("Test " + jobName + " returned error");
			System.exit(1);
		} else {
			LOGGER.info("Test " + jobName + " successful");
		}
		return Utils.printCounters(job.getCounters());
	}

	/**
	 * Get record for a given target without use of indexes
	 * 
	 * @param indexColumn
	 * @param strInputPath
	 * @param strOutputPath
	 * @param targetValue
	 * @return Metadata
	 * @throws Exception
	 */
	public HashMap<String, String> queryWithoutIndex(int indexColumn,
			String strInputPath, String strOutputPath, String targetValue)
			throws Exception {

		String jobName = "queryWithoutIndex";
		LOGGER.info("Will now execute " + jobName + " job");

		Configuration conf = new Configuration();
		conf.set("target.lookup", targetValue);
		conf.setInt("indexed.column", indexColumn);
		conf.set("mapred.job.priority", "VERY_LOW");

		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputPath + File.separator + targetValue);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		Job job = new Job(conf, jobName);
		job.setJarByClass(Query.class);
		job.setMapperClass(QueryMapper.class);
		job.setReducerClass(QueryReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(false)) {
			LOGGER.error("Test " + jobName + " returned error");
			System.exit(1);
		} else {
			LOGGER.info("Test " + jobName + " successful");
		}
		return Utils.printCounters(job.getCounters());
	}

	/**
	 * Rebuild index
	 * 
	 * @param indexColumn
	 * @param strInputPath
	 * @param strOutputPath
	 * @return Metadata
	 * @throws Exception
	 */
	public HashMap<String, String> rebuildIndex(int indexColumn,
			String strInputPath, String strOutputPath, int numReduce)
			throws Exception {

		String jobName = "rebuildIndex";
		LOGGER.info("Will now execute " + jobName + " job");

		Configuration conf = new Configuration();
		conf.setInt("indexed.column", indexColumn);
		conf.set("mapred.job.priority", "VERY_LOW");

		Path inputPath = new Path(strInputPath);
		Path outputPath = new Path(strOutputPath);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete((outputPath), true);
		}

		Job job = new Job(conf, jobName);
		job.setJarByClass(RebuildIndex.class);
		job.setMapperClass(RebuildIndexMapper.class);
		job.setCombinerClass(RebuildIndexCombiner.class);
		job.setReducerClass(RebuildIndexReducer.class);
		job.setNumReduceTasks(numReduce);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(false)) {
			LOGGER.error("Test " + jobName + " returned error");
			System.exit(1);
		} else {
			LOGGER.info("Test " + jobName + " successful");
		}
		return Utils.printCounters(job.getCounters());
	}

}
