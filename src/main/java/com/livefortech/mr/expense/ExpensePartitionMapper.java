/**
 * 
 */
package com.livefortech.mr.expense;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author ravikumarvish
 *
 */
public class ExpensePartitionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable linenumber, Text textValue, OutputCollector<Text, LongWritable> outputCollector, Reporter arg3)
			throws IOException {
		if(textValue != null) {
			String value = textValue.toString();
			System.out.println("text value : "+value);
			if(!value.isEmpty()) {
				String[] values = value.split(" ");
				
				if(values.length ==3) {
					System.out.println("values-0 : "+values[0]);
					System.out.println("values-1 : "+values[1]);
					System.out.println("values-2 : "+values[2]);
					Text department = new Text(values[1]);
					LongWritable empId = new LongWritable(new Long(values[0]).longValue());
					LongWritable expense = new LongWritable(new Long(values[2]).longValue());
					outputCollector.collect(department,expense);
				}
			}
		}
		
	}

}
