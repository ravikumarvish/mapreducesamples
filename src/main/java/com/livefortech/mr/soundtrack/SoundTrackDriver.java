/**
 * 
 */
package com.livefortech.mr.soundtrack;

import java.io.IOException;
import java.util.Iterator;

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
public class SoundTrackDriver {

	/**
	 * @param args
	 * @throws IOException 
	 */
	
	// problem to finding out unique listeners per track
	//UserId|TrackId|Shared|Radio|Skip
	//11115|222|0|1|0
	public static void main(String[] args) throws IOException {

		
		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf,SoundTrackDriver.class);
		jobConf.setJarByClass(SoundTrackDriver.class);
		
		jobConf.setMapperClass(SoundTrackMapper.class);
		jobConf.setMapOutputKeyClass(IntWritable.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		
		jobConf.setReducerClass(SoundTrackReducer.class);
		jobConf.setOutputKeyClass(IntWritable.class);
		jobConf.setOutputValueClass(Iterator.class);
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		JobClient.runJob(jobConf);
		
		
		
		

	

	}

}
