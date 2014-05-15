package com.aamend.hadoop.mapreduce.designpattern.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aamend.hadoop.mapreduce.designpattern.job.InternetAccessCount.InternetAccessParameters;

public class MRDPUtils {

	public static String CSV_SEPARATOR = ",";

	public static Map<String, String> xmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}
		return map;
	}

	public static String[] csvToStringArray(String csvLine) {

		Pattern csvPattern = Pattern.compile("\"([^\"]*)\"|(?<="
				+ CSV_SEPARATOR + "|^)([^" + CSV_SEPARATOR + "]*)(?:"
				+ CSV_SEPARATOR + "|$)");
		ArrayList<String> allMatches = new ArrayList<String>();
		Matcher matcher = null;
		int size;
		matcher = csvPattern.matcher(csvLine);
		allMatches.clear();
		String match;
		while (matcher.find()) {
			match = matcher.group(1);
			if (match != null) {
				allMatches.add(match);
			} else {
				allMatches.add(matcher.group(2));
			}
		}

		size = allMatches.size();
		if (size > 0) {
			return allMatches.toArray(new String[size]);
		} else {
			return new String[0];
		}
	}

	public static HashMap<String, String> StringArrayToHashMap(String[] csv) {

		HashMap<String, String> map = new HashMap<String, String>(
				InternetAccessParameters.MY_CSV_ARRAY.length);
		int i = 0;
		for (String string : InternetAccessParameters.MY_CSV_ARRAY) {
			map.put(string, csv[i]);
			i++;
		}
		
		return map;
	}

}