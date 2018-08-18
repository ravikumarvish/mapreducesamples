/**
 * 
 */
package com.livefortech.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ravikumarvish
 *
 */
public class WordCountReverseMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] values = value.toString().split("\t");
		IntWritable intKey = new IntWritable(Integer.parseInt(values[1]));
		context.write(intKey, new Text(values[0]));
	}
}
