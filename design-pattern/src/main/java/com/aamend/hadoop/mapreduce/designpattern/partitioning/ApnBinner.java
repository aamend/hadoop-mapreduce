package com.aamend.hadoop.mapreduce.designpattern.partitioning;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.aamend.hadoop.mapreduce.designpattern.job.MRDPUtils;

public class ApnBinner {

	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
		
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "APN Binning");
		
		job.setJarByClass(ApnBinner.class);
		job.setMapperClass(ApnBinnerMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		// The name is essentially the output directory of the job
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
				Text.class, NullWritable.class);
		
		MultipleOutputs.setCountersEnabled(job, true);
		
		job.waitForCompletion(true);
	
		
	}
	
	public class ApnBinnerMapper extends Mapper<Object, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos = null;
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] array = MRDPUtils.csvToStringArray(value.toString());
			String apn = array[7];

			mos.write(value, NullWritable.get(), apn);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

	}
	
	
}
