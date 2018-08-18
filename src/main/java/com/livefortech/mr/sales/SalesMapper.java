/**
 * 
 */
package com.livefortech.mr.sales;

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
public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable lineNumber, Text lineValue, OutputCollector<Text, IntWritable> outputCollector, Reporter arg3)
			throws IOException {
		String line = "";
		if(lineValue != null ){
			line = lineValue.toString();
		}	
		if(!line.contains("Transaction_date,Product")) {
			String[] words = line.split(",");
			String country = words[7];
			outputCollector.collect(new Text(country), new IntWritable(1));
		}
	}
}
