package com.aamend.hadoop.mapreduce.utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CsvParser {

	public static final String CSV_SEPARATOR = ",";
	
    /*
     * This Pattern will match on either quoted text or text between commas, including
     * whitespace, and accounting for beginning and end of line.
     */
    private final Pattern csvPattern = 
    		Pattern.compile("\"([^\"]*)\"|(?<="+
			    CSV_SEPARATOR+"|^)([^"+
			    CSV_SEPARATOR+"]*)(?:"+
			    CSV_SEPARATOR+"|$)");	
    private ArrayList<String> allMatches = null;	
    private Matcher matcher = null;
    private int size;

    public CsvParser() {		
    	allMatches = new ArrayList<String>();
    	matcher = null;
    	
    	
    }

    public String[] parse(String csvLine) {
    	matcher = csvPattern.matcher(csvLine);
    	allMatches.clear();
    	String match;
    	while (matcher.find()) {
    		match = matcher.group(1);
    		if (match!=null) {
    			allMatches.add(match);
    		}
    		else {
    			allMatches.add(matcher.group(2));
    		}
    	}

    	size = allMatches.size();		
    	if (size > 0) {
    		return allMatches.toArray(new String[size]);
    	}
    	else {
    		return new String[0];
    	}			
    }	
}