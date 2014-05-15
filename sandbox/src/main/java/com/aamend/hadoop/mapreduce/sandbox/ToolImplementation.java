package com.aamend.hadoop.mapreduce.sandbox;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ToolImplementation extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner
                .run(new Configuration(), new ToolImplementation(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "Tool Job");
        job.setJarByClass(ToolImplementation.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class WordCountMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        private String foo;

        @Override
        public void setup(Context context) {
            foo = context.getConfiguration().get("foo");
            if (StringUtils.isEmpty(foo)) {
                foo = "DEFAULT";
            }
            System.out.println("foo equals " + foo);
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] csv = value.toString().split(",");
            for (String str : csv) {
                word.set(str);
                context.write(word, ONE);
            }
        }
    }

    public static class WordCountReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private String foo;

        @Override
        public void setup(Context context) {
            foo = context.getConfiguration().get("foo");
            if (StringUtils.isEmpty(foo)) {
                foo = "DEFAULT";
            }
            System.out.println("foo equals " + foo);
        }

        @Override
        public void reduce(Text text, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(text, new IntWritable(sum));
        }
    }
}