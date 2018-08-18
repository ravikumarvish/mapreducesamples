/**
 * 
 */
package com.livefortech.mr.soundtrack;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author ravikumarvish
 *
 */
public class SoundTrackMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	public void map(LongWritable lineNumber, Text lineValue, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter arg3)
			throws IOException {
		String line = "";
		if(lineValue != null ){
			line = lineValue.toString();
		}
		System.out.println("Line is "+line);
		if(!line.isEmpty()) {
			String[] words = line.split("[|]");
			System.out.println("words[0]"+words[0]);
			System.out.println("words[1]"+words[1]);
			if(!words[1].isEmpty() && !words[0].isEmpty()) {
			int trackId = Integer.parseInt(words[1]);
			int userId = Integer.parseInt(words[0]);
			System.out.println("userId : "+userId+ " trackId :"+trackId);
			outputCollector.collect(new IntWritable(userId), new IntWritable(trackId));
		}
		}
	}

}
