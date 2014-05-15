package com.aamend.hadoop.mapreduce.inputsplit.format;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import com.aamend.hadoop.mapreduce.inputsplit.utils.Utils;

public class MatchingFileInputFormat extends
		FileInputFormat<LongWritable, Text> {

	private static final Logger LOGGER = Logger
			.getLogger(MatchingFileInputFormat.class);

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {

		byte[] recordDelimiterBytes = null;
		String delimiter = context.getConfiguration().get(
				"textinputformat.record.delimiter");
		if (null != delimiter) {
			recordDelimiterBytes = delimiter.getBytes();
		}
		return new LineRecordReader(recordDelimiterBytes);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		Configuration conf = job.getConfiguration();
		List<InputSplit> totalInputSplits = super.getSplits(job);
		List<InputSplit> matchingSplits = Utils.removeNonMatchingInputSplits(
				totalInputSplits, conf);
		
		LOGGER.info("Found " + matchingSplits.size()
				+ " input splits matching pattern (" + conf.get("file.pattern")
				+ ") on " + totalInputSplits.size() + " availables");

		return matchingSplits;
	}

}
