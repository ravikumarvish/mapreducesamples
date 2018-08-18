package com.livefortech.mr.expense;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

public class ExpensePartitioner extends MapReduceBase implements Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text department, LongWritable expense, int partitionCount) {
		if(department.toString().equals("sales"))
			return 0;
		else
			if(department.toString().equals("engineering"))
				return 1;
		return partitionCount;
	}

}
