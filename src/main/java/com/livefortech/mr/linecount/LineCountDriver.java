package com.livefortech.mr.linecount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author ravikumarvish
 *
 */
public class LineCountDriver {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf,LineCountDriver.class);
		jobConf.setJarByClass(LineCountDriver.class);
		jobConf.setMapperClass(LineCountMapper.class);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		
		jobConf.setReducerClass(LineCountReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

		JobClient.runJob(jobConf);
	}

}
