/**
 * 
 */
package com.livefortech.mr.sales;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author ravikumarvish
 *
 */
public class CountryMostSalesReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text country, Iterator<LongWritable> listOfProducts, OutputCollector<Text, LongWritable> outputCollector, Reporter arg3)
			throws IOException {
		long sum = 0;
		while (listOfProducts.hasNext()) {
			LongWritable count = (LongWritable) listOfProducts.next();
			sum = sum + count.get();
		}
		outputCollector.collect(country, new LongWritable(sum));
	}

}
