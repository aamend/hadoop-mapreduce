package com.aamend.hadoop.mapreduce.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by antoine on 05/06/14.
 */
public class Delimiter {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter","\n\n");
        Job job = new Job(conf, "Delimiter");
        job.setJarByClass(Delimiter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DelimiterMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class DelimiterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private final Text TEXT = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Should be a 2 lines key value
            TEXT.set(key.get() + " ******************\n" + value.toString());
            context.write(NullWritable.get(), TEXT);
        }
    }

}
