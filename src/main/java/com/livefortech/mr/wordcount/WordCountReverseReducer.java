/**
 * 
 */
package com.livefortech.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author ravikumarvish
 *
 */
public class WordCountReverseReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

	@Override
	protected void reduce(IntWritable count, Iterable<Text> values,
			Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value, count);
		}
	}
}
