/**
 * 
 */
package com.livefortech.mr.abc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author ravikumarvish
 *
 */
public class ABCReducer extends  Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int totalAmount = 0;
        for (Text value : values) {
            String valueString = value.toString();
            int currentAmount = 0;

            try {
                currentAmount = Integer.parseInt(valueString);
            } catch (NumberFormatException exception) {
                // If you have your own loggin system log it here.
                // logger.error("Blah blah blah");
            }

            totalAmount += currentAmount;
        }

        context.write(key, new Text(Integer.toString(totalAmount)));

    }

}
