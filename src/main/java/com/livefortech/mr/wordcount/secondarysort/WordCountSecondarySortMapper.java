/**
 * 
 */
package com.livefortech.mr.wordcount.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * @author ravikumarvish
 *
 */
public class WordCountSecondarySortMapper extends  Mapper<LongWritable, Text, WordCountPair, IntWritable> {

	@Override
	protected void map(LongWritable key, Text textLine, Mapper<LongWritable, Text, WordCountPair, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if(textLine != null && textLine.getLength() > 0) {
			String line = textLine.toString();
			if(!line.isEmpty()) {
				String[] values = line.split(" ");
				for (String value : values) {
					WordCountPair wordPairKey = new WordCountPair(value.toString(), 1);
					context.write(wordPairKey, new IntWritable(1));
				}
			}
		}
	}

}
