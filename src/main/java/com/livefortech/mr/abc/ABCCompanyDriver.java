/**
 * 
 */
package com.livefortech.mr.abc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





/**
 * @author ravikumarvish
 *
 */
public class ABCCompanyDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	
	// problem to finding total amount received from same customer
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ABC Sample Job");
        job.setJarByClass(ABCCompanyDriver.class);
        
        job.setReducerClass(ABCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), AirlineInputFormat.class,SampleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), BookInputFormat.class,SampleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), MobileInputFormat.class,SampleMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
		
		

	

	}

}
