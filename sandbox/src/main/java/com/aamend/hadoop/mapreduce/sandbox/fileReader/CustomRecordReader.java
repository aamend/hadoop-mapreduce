package com.aamend.hadoop.mapreduce.sandbox.fileReader;

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

import java.io.IOException;

public class CustomRecordReader
        extends RecordReader<LongWritable, Text> {

    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    private static final Log LOG = LogFactory.getLog(
            CustomRecordReader.class);

    @Override
    public void initialize(
            InputSplit genericSplit,
            TaskAttemptContext context)
            throws IOException {

        FileSplit split = (FileSplit) genericSplit;

        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());


        in = new LineReader(fileIn, job);
        Text firstLine = new Text();
        Text lastLine = new Text();
        in.readLine(firstLine, maxLineLength, (int) start);
        in.readLine(lastLine, maxLineLength, (int) end);

        LOG.info("First Line: " + firstLine);
        LOG.info("LastLine: " + lastLine);

        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException {

        key.set(pos);
        int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(value, maxLineLength,
                    Math.max((int) Math.min(
                                    Integer.MAX_VALUE, end - pos),
                            maxLineLength));

            if (newSize == 0) {
                break;
            }

            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }

            LOG.info("Skipped line of size " +
                    newSize + " at pos "
                    + (pos - newSize));
        }

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
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

}
