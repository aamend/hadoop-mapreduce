package com.aamend.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.aamend.hadoop.mapreduce.SMSCDRMapper.CDRCounter;

public class SMSCDRMapperReducerTest {

	public static String CDR_LINE = "655209;1;796764372490213;804422938115889;6";
	public static String CDR_EXPECTED_KEY = "6";
	public static int EXPECTED_COUNTER = 0;
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	
	@Before
	public void setUp() {
		SMSCDRMapper mapper = new SMSCDRMapper();
		SMSCDRReducer reducer = new SMSCDRReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.getConfiguration().set("param1", "param1");
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(CDR_LINE));
		mapDriver.withOutput(new Text(CDR_EXPECTED_KEY), new IntWritable(1));
		mapDriver.runTest();
		assertEquals("Expected counter null", EXPECTED_COUNTER, mapDriver.getCounters().findCounter(CDRCounter.NonSMSCDR).getValue());
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text(CDR_EXPECTED_KEY), values);
		reduceDriver.withOutput(new Text(CDR_EXPECTED_KEY), new IntWritable(2));
		reduceDriver.runTest();
	}
}