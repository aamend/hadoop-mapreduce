package com.aamend.hadoop.mapreduce.job;

import org.apache.hadoop.io.Text;

public interface RadiusParameters {

	public enum EventType {

		START("start"), STOP("stop"), UPDATE("interim-update"), UNKNOWN(
				"unknown");
		private String value;

		private EventType(String value) {
			this.value = value;
		}

		public String getName() {
			return toString();
		}

		public String getValue() {
			return value;
		}

	}

	public static final String RADIUS_DATE_FORMAT = "^[A-Za-z]{3},\\s\\d{2}\\s[A-Za-z]{3}\\s\\d{4}\\s\\d{2}:\\d{2}:\\d{2}.*";
	public static final String RADIUS_TYPE = "Acct-Status-Type";
	public static final String RADIUS_SESSION = "Acct-Session-Id";
	public static final String RADIUS_TIMESTAMP = "Event-Timestamp";
	public static final String RADIUS_IP = "Framed-IP-Address";
	public static final String RADIUS_UNIT = "Called-Station-Id";
	public static final String RADIUS_MSISDN = "Calling-Station-Id";
	public static final String RADIUS_HOSTNAME = "NAS-Identifier";
	public static final String RADIUS_OUTPUT = "Acct-Output-Octets";
	public static final String RADIUS_INPUT = "Acct-Input-Octets";
	public static final String RADIUS_DURATION = "Acct-Session-Time";

	public static final String NOT_FOUND = "NA";
	public static final String CSV_DELIM = ",";
	public final static Text EOL = new Text("\n");

	public String[] RADIUS_ARRAY = new String[] { "Acct-Status-Type",
			"Event-Timestamp", "Acct-Session-Time", "NAS-Identifier",
			"Calling-Station-Id", "Called-Station-Id", "Framed-IP-Address",
			"Acct-Input-Octets", "Acct-Output-Octets", "Acct-Session-Id" };

	public String[] CSV_ARRAY = new String[] { "START_TIME", "STOP_TIME", "IP",
			"UNIT", "HOSTNAME", "INPUT", "OUTPUT", "MSISDN", "SESSION_ID" };

}
