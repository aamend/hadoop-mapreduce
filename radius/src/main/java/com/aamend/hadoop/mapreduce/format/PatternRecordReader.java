package com.aamend.hadoop.mapreduce.format;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class PatternRecordReader extends RecordReader<LongWritable, Text> {

	private LineReader in;
	private long start;
	private long pos;
	private long end;
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private final static Text EOL = new Text("\n");
	private Pattern delimiterPattern;
	private String delimiterRegex;
	private int maxLengthRecord;
	// private Text lastDelimValue = new Text();

	private static final Log LOG = LogFactory.getLog(RecordReader.class);

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		// Retrieve configuration value
		Configuration job = context.getConfiguration();
		this.delimiterRegex = job.get("record.delimiter.regex");
		this.maxLengthRecord = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);

		// Compile pattern only once per InputSplit
		delimiterPattern = Pattern.compile(delimiterRegex);

		// Retrieve FileSplit details
		FileSplit fileSplit = (FileSplit) split;
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		final Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(job);

		// Open FileSplit FSDataInputStream
		FSDataInputStream fileIn = fs.open(fileSplit.getPath());

		// Skip first record if Split does not start at byte 0 (first line of
		// file)
		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}

		// Read FileSplit content
		in = new LineReader(fileIn, job);
		if (skipFirstLine) {
			LOG.info("Need to skip first line of Split");
			Text dummy = new Text();
			start += readNext(dummy, 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}

		this.pos = start;

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		key.set(pos);
		int newSize = 0;

		// Get only the records for which the first byte
		// is located before end of current Split
		while (pos < end) {

			// Read new record and store content into value
			newSize = readNext(value, maxLengthRecord, Math.max(
					(int) Math.min(Integer.MAX_VALUE, end - pos),
					maxLengthRecord));

			pos += newSize;
			if (newSize == 0) {
				break;
			}
			if (newSize < maxLengthRecord) {
				break;
			}

			LOG.error("Skipped radius of size " + newSize + " at pos "
					+ (pos - newSize));
		}

		// No bytes to read (end of split)
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return start == end ? 0.0f : Math.min(1.0f, (pos - start)
				/ (float) (end - start));
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	private int readNext(Text text, int maxLineLength, int maxBytesToConsume)
			throws IOException {

		int offset = 0;
		text.clear();
		Text tmp = new Text();

		for (int i = 0; i < maxBytesToConsume; i++) {

			int offsetTmp = in.readLine(tmp, maxLineLength, maxBytesToConsume);
			offset += offsetTmp;
			Matcher m = delimiterPattern.matcher(tmp.toString());

			// End of File
			if (offsetTmp == 0) {
				break;
			}

			if (m.matches()) {
				break;
			} else {
				// Append value to record
				text.append(EOL.getBytes(), 0, EOL.getLength());
				text.append(tmp.getBytes(), 0, tmp.getLength());
			}
		}

		return offset;
	}

}
