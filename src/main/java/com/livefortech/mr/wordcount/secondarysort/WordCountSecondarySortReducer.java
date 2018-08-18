/**
 * 
 */
package com.livefortech.mr.wordcount.secondarysort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author ravikumarvish
 *
 */
public class WordCountSecondarySortReducer extends Reducer<WordCountPair, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(WordCountPair text, Iterable<IntWritable> values,
			Reducer<WordCountPair, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum = sum + value.get();	    
		}
		
		context.write(new Text(text.word), new IntWritable(sum));
	}

}
