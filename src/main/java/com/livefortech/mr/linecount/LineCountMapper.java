/**
 * 
 */
package com.livefortech.mr.linecount;

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
public class LineCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable line, Text lineText, OutputCollector<Text, IntWritable> outputCollector, Reporter arg3)
			throws IOException {
		System.out.println("line no : "+line);
		System.out.println("line value is : "+lineText);
		outputCollector.collect(new Text("New Line"), new IntWritable(1));
		
	}

}
