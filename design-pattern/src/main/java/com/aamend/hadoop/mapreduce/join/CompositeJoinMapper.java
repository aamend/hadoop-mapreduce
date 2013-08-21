package com.aamend.hadoop.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;

public class CompositeJoinMapper extends MapReduceBase implements
		Mapper<Text, TupleWritable, Text, Text> {

	@Override
	public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Get the first two elements in the tuple and output them
		output.collect((Text) value.get(0), (Text) value.get(1));

	}

}
