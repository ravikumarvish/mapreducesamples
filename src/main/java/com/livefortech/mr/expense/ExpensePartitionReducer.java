package com.livefortech.mr.expense;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExpensePartitionReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void reduce(Text department, Iterator<LongWritable> expenses, OutputCollector<Text, LongWritable> outputCollector, Reporter arg3)
			throws IOException {
		long totalExpense = 0;
		while(expenses.hasNext()) {
			totalExpense = totalExpense + expenses.next().get();
		}
		
		outputCollector.collect(department, new LongWritable(totalExpense));
		
	}

}
