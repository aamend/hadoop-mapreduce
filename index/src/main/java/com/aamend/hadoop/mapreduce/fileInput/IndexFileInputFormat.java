package com.aamend.hadoop.mapreduce.fileInput;

import java.io.IOException;
import java.util.List;

import com.aamend.hadoop.mapreduce.utils.Utils;
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

public class IndexFileInputFormat extends FileInputFormat<LongWritable, Text> {

	private static final Logger LOGGER = Logger
			.getLogger(IndexFileInputFormat.class);

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new LineRecordReader();
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
		List<InputSplit> indexInputSplits;
		try {
			indexInputSplits = Utils.removeNonIndexedInputSplits(
                    totalInputSplits, conf);
		} catch (Exception e) {
			throw new IOException(e);
		}
		LOGGER.info("Found " + indexInputSplits.size()
				+ " indexed input splits on " + totalInputSplits.size()
				+ " availables");
		return indexInputSplits;
	}

}