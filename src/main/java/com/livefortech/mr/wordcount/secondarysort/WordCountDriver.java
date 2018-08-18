/**
 * 
 */
package com.livefortech.mr.wordcount.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author ravikumarvish
 *
 */
public class WordCountDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job jobConf1 = new Job(conf);
		jobConf1.setJarByClass(WordCountDriver.class);
		
		jobConf1.setMapperClass(WordCountSecondarySortMapper.class);
		jobConf1.setMapOutputKeyClass(WordCountPair.class);
		jobConf1.setMapOutputValueClass(IntWritable.class);
		
		// Partitioning/Sorting/Grouping configuration
		jobConf1.setPartitionerClass(NaturalKeyPartitioner.class);
		
		
			
		
		jobConf1.setReducerClass(WordCountSecondarySortReducer.class);
		jobConf1.setOutputKeyClass(Text.class);
		jobConf1.setOutputValueClass(IntWritable.class);
		jobConf1.setGroupingComparatorClass(WordPairCompartor.class);
		jobConf1.setSortComparatorClass(FullKeyComparator.class);
		jobConf1.setNumReduceTasks(1);
		
		//jobConf1.setSortComparatorClass(IntComparator.class);
		
		FileInputFormat.addInputPath(jobConf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf1, new Path(args[1]));
		
		boolean result = jobConf1.waitForCompletion(true);

		System.exit(result ? 0 : 1);

	}

}
