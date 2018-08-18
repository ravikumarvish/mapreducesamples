/**
 * 
 */
package com.livefortech.mr.linecount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author ravikumarvish
 *
 */
public class LineCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> ouputCollector, Reporter arg3)
			throws IOException {
		int count = 0;
		while(values.hasNext()) {
			IntWritable value = values.next();
			count = count + value.get();
		}
		ouputCollector.collect(key,new IntWritable(count));
		
	}

}
