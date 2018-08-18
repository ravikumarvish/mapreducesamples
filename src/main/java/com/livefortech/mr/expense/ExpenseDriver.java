package com.livefortech.mr.expense;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author ravikumarvish
 *
 */
public class ExpenseDriver {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf,ExpenseDriver.class);
		jobConf.setJarByClass(ExpenseDriver.class);
		jobConf.setMapperClass(ExpenseMapper.class);
		jobConf.setMapOutputKeyClass(LongWritable.class);
		jobConf.setMapOutputValueClass(LongWritable.class);
		
		jobConf.setReducerClass(ExpenseReducer.class);
		jobConf.setOutputKeyClass(LongWritable.class);
		jobConf.setOutputValueClass(LongWritable.class);
		
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		JobClient.runJob(jobConf);
	}

}
