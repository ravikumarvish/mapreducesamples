/**
 * 
 */
package com.livefortech.mr.expense;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author ravikumarvish
 *
 */
public class ExpenseReducer extends MapReduceBase
		implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable empId, Iterator<LongWritable> expenses, OutputCollector<LongWritable, LongWritable> outputCollector,
			Reporter arg3) throws IOException {
		long totalExpense = 0;
		while(expenses.hasNext()) {
			totalExpense = totalExpense + expenses.next().get();
		}
		
		outputCollector.collect(empId, new LongWritable(totalExpense));
		
	}

}
