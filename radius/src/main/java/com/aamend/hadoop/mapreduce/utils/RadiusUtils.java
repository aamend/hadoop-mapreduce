package com.aamend.hadoop.mapreduce.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.aamend.hadoop.mapreduce.job.RadiusParameters.EventType;

public class RadiusUtils {

	public static EventType stringToEventType(String str) {
		if (str == null) {
			return EventType.UNKNOWN;
		} else {
			for (EventType ev : EventType.values()) {
				if (str.equalsIgnoreCase(ev.getValue())) {
					return ev;
				}
			}
			return EventType.UNKNOWN;
		}
	}
	
	public static String finalCsv(String stop, String start) {
		return stop + start;
	}

	public static long stringDateToTimestamp(String strDate)
			throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
		Date convertedDate = dateFormat.parse(strDate);
		return convertedDate.getTime() / 1000;
	}
	
	public static Date convertEpoch(long epoch) {
		Date date = new Date(epoch * 1000);
		return date;
	}

	public static String prettyPrintDate(Date date) {
		DateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		String formatted = format.format(date);
		return formatted;
	}
}
