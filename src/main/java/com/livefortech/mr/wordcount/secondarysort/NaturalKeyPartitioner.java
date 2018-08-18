package com.livefortech.mr.wordcount.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<WordCountPair, IntWritable> {

	@Override
	public int getPartition(WordCountPair key, IntWritable arg1, int numPartitions) {
		return Math.abs(key.word.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
