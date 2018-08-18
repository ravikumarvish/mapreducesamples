/**
 * 
 */
package com.livefortech.mr.soundtrack;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
public class SoundTrackReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, Text> {

	@Override
	public void reduce(IntWritable trackId, Iterator<IntWritable> tracks, OutputCollector<IntWritable, Text> output,
			Reporter arg3) throws IOException {
		
		Set<Integer> uniqueUsers = new HashSet<Integer>();
		System.out.println("track Id : "+trackId);
		while(tracks.hasNext()) {
			Integer userId = tracks.next().get();
			System.out.println("user Id :"+userId);
			uniqueUsers.add(userId);
		}
		String values = "";
		for (Integer user : uniqueUsers) {
			values = values+ "\t" + user.toString();
		}
		output.collect(trackId, new Text(values));
		
	}

}
