package com.livefortech.mr.abc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class SampleMapper extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
    	System.out.println("key : "+key+" value : "+value);
            context.write(key, value);
    }

}
