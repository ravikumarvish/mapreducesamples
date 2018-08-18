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
public class CountryMostSalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

	static enum INVALIDAMOUNT { INVALID };
	
	@Override
	public void map(LongWritable lineNumber, Text lineValue, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter)
			throws IOException {
		String line = "";
		if(lineValue != null ){
			line = lineValue.toString();
		}	
		if(!line.contains("Transaction_date,Product")) {
			//System.out.println("Line : "+line);
			String[] words = line.split(",");
			String country = words[7];
			String price = words[2];
			if(price.contains("\""))
				reporter.incrCounter(INVALIDAMOUNT.INVALID, 1);
			else
				outputCollector.collect(new Text(country), new LongWritable(new Long(price)));
		}
	}
}
