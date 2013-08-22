package com.aamend.hadoop.mapreduce.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

public class Utils {

	public static List<InputSplit> removeNonIndexedInputSplits(
			List<InputSplit> allInputSplits, Configuration conf)
			throws Exception {

		Path path = new Path(conf.get("index.output.path"));
		List<String> indexedHashedSplits = readInputSplitsFromFile(conf, path);
		List<InputSplit> indexedSplits = new ArrayList<InputSplit>();

		// Filter out any input split that is not in indexed list
		for (InputSplit split : allInputSplits) {
			if (indexedHashedSplits.contains(inputSplitToString(split))) {
				indexedSplits.add(split);
			}
		}

		// Return list of input Split to query
		return indexedSplits;
	}

	public static HashMap<String, String> printCounters(Counters counters) {
		HashMap<String, String> map = new HashMap<String, String>();
		for (String name : counters.getGroupNames()) {
			for (Counter counter : counters.getGroup(name)) {
				map.put(counter.getName().toString(),
						String.valueOf(counter.getValue()));
			}
		}
		return map;
	}

	public static String inputSplitToString(InputSplit split) {
		return MD5Hash.digest(split.toString()).toString();
	}

	public static List<String> readInputSplitsFromFile(Configuration conf,
			Path path) throws Exception {

		List<String> inputSplits = new ArrayList<String>();
		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(path)){
            FileStatus fileStatus = hdfs.getFileStatus(path);
            if(!fileStatus.isDir()){
                throw new Exception("Path " + path + " is not a directory");
            }
        } else {
            throw new Exception("Path " + path + " does not exist");
        }

		FileStatus[] fss = hdfs.globStatus(new Path(path + "/part-*"));
		for (FileStatus status : fss) {

			Path partFile = status.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, conf);

			// Configure Sequence file
			Writable key = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);

			// Read Sequence file and write each line to Output Stream
			reader.getPosition();
			while (reader.next(key, value)) {
				inputSplits.add(key.toString());
				reader.getPosition();
			}
			reader.close();
		}

		return inputSplits;
	}

}
