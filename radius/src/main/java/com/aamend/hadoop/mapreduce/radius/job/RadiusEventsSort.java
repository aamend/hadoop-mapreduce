package com.aamend.hadoop.mapreduce.radius.job;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.aamend.hadoop.mapreduce.radius.format.PatternInputFormat;
import com.aamend.hadoop.mapreduce.radius.utils.RadiusUtils;

public class RadiusEventsSort extends Configured implements Tool {

	private static final Logger LOGGER = Logger
			.getLogger(RadiusEventsSort.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		RadiusEventsSort sort = new RadiusEventsSort();
		int res = ToolRunner.run(conf, sort, args);
		System.exit(res);
	}

	private static void printUsage() {
		String className = RadiusEventsSort.class.getCanonicalName();
		LOGGER.error("Usage: hadoop jar %JAR% " + className
				+ " [Generic options]" + " [<inputPath> <outputDir> "
				+ "<targetType (0-MSISDN, 1-IP)> "
				+ "<targetValue (all)> <fromDate> <toDate>]");
		ToolRunner.printGenericCommandUsage(System.err);
		System.exit(-1);
	}

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath = null;
		Path outputDir = null;
		int targetType = 0;
		long fromEpoch = 0;
		long toEpoch = 0;
		String targetValue = null;

		try {

			// Input output path
			inputPath = new Path(args[0]);
			outputDir = new Path(args[1]);

			// Query parameters
			try {
				targetType = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				LOGGER.error("TargetType [" + args[2] + "] is not correct");
				printUsage();
			}

			targetValue = args[3];
			fromEpoch = RadiusUtils.stringDateToTimestamp(args[4]);
			toEpoch = RadiusUtils.stringDateToTimestamp(args[5]);

		} catch (ArrayIndexOutOfBoundsException e) {
			printUsage();
		}

		// Configuration set thanks to Tool interface
		Configuration conf = this.getConf();
		conf.set("record.delimiter.regex", RadiusParameters.RADIUS_DATE_FORMAT);
		conf.setInt("target.type.lookup", targetType);
		conf.set("target.value.lookup", targetValue);
		conf.setLong("target.from.timestamp", fromEpoch);
		conf.setLong("target.to.timestamp", toEpoch);

		// Create job
		Job job = new Job(conf, "RADIUS_" + UUID.randomUUID().toString());
		job.setJarByClass(RadiusParserMapper.class);

		// Setup map-only job
		job.setMapperClass(RadiusParserMapper.class);
		job.setReducerClass(RadiusParserReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(PatternInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output directory if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;

		Path reducerPath = new Path(outputDir + "/part-r-00000");
		if (hdfs.exists(reducerPath)) {
			FSDataInputStream fis = hdfs.open(reducerPath);
			Scanner scan = new Scanner(fis);
			while (scan.hasNext()) {
				System.out.println(scan.nextLine());
			}
			fis.close();
		}

		return code;

	}

	public static class RadiusParserMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();
		private long fromTimestamp;
		private long toTimestamp;
		private String targetValue;
		private int targetType;

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			fromTimestamp = conf.getLong("target.from.timestamp", 0);
			toTimestamp = conf.getLong("target.to.timestamp", 0);
			targetValue = conf.get("target.value.lookup");
			targetType = conf.getInt("target.type.lookup", 0);
		}

		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {

			// -----------------------------------------------
			// PARSE RADIUS EVENT
			// -----------------------------------------------

			// Populate HashMap with all required values
			Map<String, String> keyValues = new HashMap<String, String>();
			for (String radiusKeyValue : value.toString().split("\n")) {
				for (String radiusKey : RadiusParameters.RADIUS_ARRAY) {
					if (radiusKeyValue.contains(radiusKey)) {
						String radiusValue = radiusKeyValue.split("=")[1];
						if (radiusValue == null) {
							radiusValue = RadiusParameters.NOT_FOUND;
						} else {
							radiusValue = radiusValue.replaceAll("\\s", "");
						}
						keyValues.put(radiusKey, radiusValue);
					}
				}
			}

			// Make sure session ID is found as this will be our key
			String strKey = keyValues.get(RadiusParameters.RADIUS_SESSION);
			if (strKey == null || strKey.equals(RadiusParameters.NOT_FOUND)) {
				LOGGER.error("Session ID not found for record ["
						+ value.toString() + "], cannot get unique identifier");
				return;
			}

			// -----------------------------------------------
			// QUERY EVENT
			// -----------------------------------------------

			RadiusParameters.EventType eventType = RadiusUtils.stringToEventType(keyValues
					.get(RadiusParameters.RADIUS_TYPE));

			// Only STOP and START events are supported.
			switch (eventType) {
			case START:
			case STOP:
				break;
			default:
				return;
			}

			// Query against target
			if (!targetValue.equals("all")) {
				String targetToCompare = null;
				switch (targetType) {
				case 0:
					// MSISDN query
					targetToCompare = keyValues
							.get(RadiusParameters.RADIUS_MSISDN);
					break;
				case 1:
					// IP query
					targetToCompare = keyValues.get(RadiusParameters.RADIUS_IP);
					break;
				default:
					String msg = "Target Type [" + targetType
							+ "] is not supported";
					LOGGER.error(msg);
					throw new IOException(msg);
				}

				if (!targetValue.equals(targetToCompare)) {
					return;
				}
			}

			// Query against date
			long epoch = Long.parseLong(keyValues
					.get(RadiusParameters.RADIUS_TIMESTAMP));
			if (epoch < fromTimestamp || epoch > toTimestamp) {
				return;
			}

			// -----------------------------------------------
			// PREPARE INTERMEDIATE CSV
			// -----------------------------------------------

			// Make sure hash is complete before building CSV
			StringBuilder csvBuilder = new StringBuilder();
			for (String csv : RadiusParameters.RADIUS_ARRAY) {
				if (keyValues.get(csv) == null) {
					keyValues.put(csv, RadiusParameters.NOT_FOUND);
				}
				csvBuilder.append(keyValues.get(csv)
						+ RadiusParameters.CSV_DELIM);
			}

			// Increment counters
			context.getCounter("Radius Events", eventType.getName()).increment(
					1);

			// Write key / value
			outKey.set(strKey);
			outValue.set(csvBuilder.toString());
			context.write(outKey, outValue);

		}

	}

	public static class RadiusParserReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		private Text outKey = new Text();
		private NullWritable outValue = NullWritable.get();

		private Map<String, String> aMap;
		private Map<String, String> bMap;

		private String sessionId;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// -----------------------------------------------
			// READ STOP + START EVENT FOR SESSION ID
			// -----------------------------------------------

			sessionId = key.toString();
			Map<String, String> keyValues = new HashMap<String, String>();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {

				String strRadiusEvent = it.next().toString();
				String strRadiusEvents[] = strRadiusEvent
						.split(RadiusParameters.CSV_DELIM);
				for (int i = 0; i < strRadiusEvents.length; i++) {
					keyValues.put(RadiusParameters.RADIUS_ARRAY[i],
							strRadiusEvents[i]);
				}

				RadiusParameters.EventType eventType = RadiusUtils.stringToEventType(keyValues
						.get(RadiusParameters.RADIUS_TYPE));

				switch (eventType) {
				case START:
					aMap = keyValues;
					break;
				case STOP:
					bMap = keyValues;
					break;
				default:
					continue;
				}
			}

			// -----------------------------------------------
			// VALIDATE RECORDS
			// -----------------------------------------------

			if (bMap == null) {
				LOGGER.error("Orphan start without stop for sessionId ["
						+ sessionId + "], ignore record");
				return;
			}

			long aEpoch;
			long bEpoch;
			int bDuration;
			Date aTime;
			Date bTime;

			try {
				aEpoch = Long.parseLong(aMap
						.get(RadiusParameters.RADIUS_TIMESTAMP));
			} catch (NumberFormatException e) {
				aEpoch = 0;
			}
			try {
				bEpoch = Long.parseLong(bMap
						.get(RadiusParameters.RADIUS_TIMESTAMP));
			} catch (NumberFormatException e) {
				bEpoch = 0;
			}
			try {
				bDuration = Integer.parseInt(bMap
						.get(RadiusParameters.RADIUS_DURATION));
			} catch (NumberFormatException e) {
				bDuration = 0;
			}

			if (bEpoch == 0) {
				if (aEpoch == 0 || bDuration == 0) {
					LOGGER.error("Invalid Date time specification for sessionId ["
							+ sessionId + "], cannot compute StopTime");
					return;
				} else {
					bEpoch = aEpoch + bDuration;
				}
			} else {
				if (aEpoch == 0) {
					if (bDuration == 0) {
						LOGGER.error("Invalid Date time specification for sessionId ["
								+ sessionId + "], cannot compute StartTime");
						return;
					} else {
						aEpoch = bEpoch - bDuration;
					}
				}
			}

			aTime = RadiusUtils.convertEpoch(aEpoch);
			bTime = RadiusUtils.convertEpoch(bEpoch);
			if (aTime == null || bTime == null) {
				LOGGER.error("Invalid Date time specification for sessionId ["
						+ sessionId + "], StopTime or StartTime is not valid");
				return;
			}

			// -----------------------------------------------
			// PREPARE FINAL CSV
			// -----------------------------------------------

			StringBuilder csv = new StringBuilder();
			csv.append(RadiusUtils.prettyPrintDate(aTime)
					+ RadiusParameters.CSV_DELIM);
			csv.append(RadiusUtils.prettyPrintDate(bTime)
					+ RadiusParameters.CSV_DELIM);
			csv.append(bMap.get(RadiusParameters.RADIUS_IP)
					+ RadiusParameters.CSV_DELIM);
			csv.append(bMap.get(RadiusParameters.RADIUS_UNIT)
					+ RadiusParameters.CSV_DELIM);
			csv.append(bMap.get(RadiusParameters.RADIUS_HOSTNAME)
					+ RadiusParameters.CSV_DELIM);
			csv.append(bMap.get(RadiusParameters.RADIUS_INPUT)
					+ RadiusParameters.CSV_DELIM);
			csv.append(bMap.get(RadiusParameters.RADIUS_OUTPUT)
					+ RadiusParameters.CSV_DELIM);
			csv.append(bMap.get(RadiusParameters.RADIUS_MSISDN)
					+ RadiusParameters.CSV_DELIM);
			csv.append(key.toString());
			String line = csv.toString();
			LOGGER.info("Saving line [" + line + "]");

			outKey.set(line);
			context.write(outKey, outValue);

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			aMap = new HashMap<String, String>();
			bMap = new HashMap<String, String>();
		}

	}
}
