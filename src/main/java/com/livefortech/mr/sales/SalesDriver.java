package com.livefortech.mr.sales;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;




public class SalesDriver {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf,SalesDriver.class);
		jobConf.setJarByClass(SalesDriver.class);
		
		jobConf.setMapperClass(SalesMapper.class);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		
		jobConf.setReducerClass(SalesReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		JobClient.runJob(jobConf);
		
		
		
		

	}

}
