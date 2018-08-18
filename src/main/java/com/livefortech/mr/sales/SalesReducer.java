/**
 * 
 */
package com.livefortech.mr.sales;

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
public class SalesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text country, Iterator<IntWritable> listOfProducts, OutputCollector<Text, IntWritable> outputCollector, Reporter arg3)
			throws IOException {
		int sum = 0;
		while (listOfProducts.hasNext()) {
			IntWritable count = (IntWritable) listOfProducts.next();
			sum = sum + count.get();
		}
		outputCollector.collect(country, new IntWritable(sum));
	}

}
