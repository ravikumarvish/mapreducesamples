/**
 * 
 */
package com.livefortech.mr.wordcount;

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
		
		jobConf1.setMapperClass(WordCountMapper.class);
		jobConf1.setMapOutputKeyClass(Text.class);
		jobConf1.setMapOutputValueClass(IntWritable.class);
		
		jobConf1.setReducerClass(WordCountReducer.class);
		jobConf1.setOutputKeyClass(Text.class);
		jobConf1.setOutputValueClass(IntWritable.class);
		
		//jobConf1.setSortComparatorClass(IntComparator.class);
		
		FileInputFormat.addInputPath(jobConf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf1, new Path(args[1]));
		
		boolean result = jobConf1.waitForCompletion(true);

		
		Job jobConf2 = new Job(conf);
		jobConf2.setJarByClass(WordCountDriver.class);
		
		jobConf2.setMapperClass(WordCountReverseMapper.class);
		jobConf2.setMapOutputKeyClass(IntWritable.class);
		jobConf2.setMapOutputValueClass(Text.class);
		
		jobConf2.setReducerClass(WordCountReverseReducer.class);
		jobConf2.setOutputKeyClass(Text.class);
		jobConf2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(jobConf2, new Path(args[1]));
		FileOutputFormat.setOutputPath(jobConf2, new Path(args[1]+"final"));
		
		result = jobConf2.waitForCompletion(true);

        System.exit(result ? 0 : 1);

	}

}
