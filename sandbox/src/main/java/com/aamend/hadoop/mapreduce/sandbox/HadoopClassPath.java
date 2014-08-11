package com.aamend.hadoop.mapreduce.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by antoine on 6/5/14.
 */
public class HadoopClassPath {

    public static void main(String[] args) throws Exception {

        // Create Hadoop configuration
        Configuration conf = new Configuration();

        // Add 3rd-party libraries
        addJarToDistributedCache(MyClass.class, conf);

        // Create my job
        Job job = new Job(conf, "Hadoop-classpath");

        job.setJarByClass(Delimiter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void addJarToDistributedCache(
            Class classToAdd, Configuration conf)
            throws IOException {

        // Retrieve jar file for class2Add
        String jar = classToAdd.getProtectionDomain().
                getCodeSource().getLocation().
                getPath();
        File jarFile = new File(jar);

        // Declare new HDFS location
        Path hdfsJar = new Path("/user/hadoopi/lib/"
                + jarFile.getName());

        // Mount HDFS
        FileSystem hdfs = FileSystem.get(conf);

        // Copy (override) jar file to HDFS
        hdfs.copyFromLocalFile(false, true,
                new Path(jar), hdfsJar);

        // Add jar to distributed classPath
        DistributedCache.addFileToClassPath(hdfsJar, conf);
    }

    public class MyClass {

    }
}
