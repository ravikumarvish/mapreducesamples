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
public class WordCountMapper extends  Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text textLine, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if(textLine != null && textLine.getLength() > 0) {
			String line = textLine.toString();
			if(!line.isEmpty()) {
				String[] values = line.split(" ");
				for (String value : values) {
					context.write(new Text(value), new IntWritable(1));
				}
			}
		}
	}

}
