package com.aamend.hadoop.mapreduce.sandbox.fileReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private Text firstLine = new Text();
    private Text lastLine = new Text();

    private static Logger LOGGER = LoggerFactory.getLogger(CustomMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(StringUtils.isEmpty(firstLine.toString())){
            firstLine.set(value.toString());
        }
        lastLine.set(value.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOGGER.info("First line is {}", firstLine.toString());
        LOGGER.info("Last line is {}", lastLine.toString());
    }


}
