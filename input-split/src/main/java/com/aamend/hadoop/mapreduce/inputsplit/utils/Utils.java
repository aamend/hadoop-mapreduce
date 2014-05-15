package com.aamend.hadoop.mapreduce.inputsplit.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

public class Utils {

	public static List<InputSplit> removeNonMatchingInputSplits(
			List<InputSplit> allInputSplits, Configuration conf) {

		Pattern filePattern = Pattern.compile(conf.get("file.pattern"));
		List<InputSplit> matchingSplits = new ArrayList<InputSplit>();
		for (InputSplit split : allInputSplits) {
			Matcher m = filePattern.matcher(split.toString());
			if (m.matches()) {
				matchingSplits.add(split);
			}
		}

		// Return list of input Split to query
		return matchingSplits;

	}

}
