package com.aamend.hadoop.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeJoin {

	static Logger LOGGER = LoggerFactory.getLogger(CompositeJoin.class);

	/*
	 * A composite join is a specialized type of join operation that can be
	 * performed on the map-side with many very large formatted inputs. However,
	 * it requires the data to be already organized or prepared in a very
	 * specific way.
	 */

	/*
	 * Composite joins are particularly useful if you want to join very large
	 * data sets together. However, the data sets must first be sorted by
	 * foreign key, partitioned by foreign key, and read in a very particular
	 * manner in order to use this type of join. With that said, if your data
	 * can be read in such a way or you can prepare your data, a composite join
	 * has a huge leg-up over the other types.
	 */

	/*
	 * This join utility is restricted to only inner and full outer joins.
	 */

	/*
	 * A composite join should be used when: • An inner or full outer join is
	 * desired. • All the data sets are sufficiently large. • All data sets can
	 * be read with the foreign key as the input key to the mapper. • All data
	 * sets have the same number of partitions. • Each partition is sorted by
	 * foreign key, and all the foreign keys reside in the associated partition
	 * of each data set. That is, partition X of data sets A and B contain the
	 * same foreign keys and these foreign keys are present only in partition X.
	 * For a visualization of this partitioning and sorting key, refer to Figure
	 * 5-3. • The data sets do not change often (if they have to be prepared).
	 */

	/*
	 * USE OLD API HERE !
	 */

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Path userPath = new Path(args[0]);
		Path commentPath = new Path(args[1]);
		Path outputDir = new Path(args[2]);
		String joinType = args[3];

		JobConf conf = new JobConf("CompositeJoin");
		conf.setJarByClass(CompositeJoin.class);
		conf.setMapperClass(CompositeJoinMapper.class);
		conf.setNumReduceTasks(0);

		// Set the input format class to a CompositeInputFormat class.
		// The CompositeInputFormat will parse all of our input files and output
		// records to our mapper.
		conf.setInputFormat(CompositeInputFormat.class);

		// The composite input format join expression will set how the records
		// are going to be read in, and in what input format.
		conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType,
				KeyValueTextInputFormat.class, userPath, commentPath));

		TextOutputFormat.setOutputPath(conf, outputDir);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			Thread.sleep(1000);
		}
		
		System.exit(job.isSuccessful() ? 0 : 1);

	}

}
