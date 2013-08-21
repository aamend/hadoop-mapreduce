package com.aamend.hadoop.mapreduce.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Asynchronous {

    public static String HADOOP_USER = "antoine";
    public static String TRACKER_URI = "localhost.localdomain:8021";
    public static String HDFS_URI = "hdfs://localhost.localdomain:8020";

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        Path inputPath = new Path("input/iac");
        Path outputDir = new Path("output/wordcount");

        // Create configuration
        Configuration conf = new Configuration(true);
        conf.set("hadoop.job.ugi", HADOOP_USER);
        conf.set("mapred.job.tracker", TRACKER_URI);
        conf.set("fs.defaultFS", HDFS_URI);

        // Create job
        Job job = new Job(conf, "WordCount");
        job.setJarByClass(Asynchronous.class);

        // Setup MapReduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);

    }

}
