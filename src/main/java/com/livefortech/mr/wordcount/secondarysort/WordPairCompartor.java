/**
 * 
 */
package com.livefortech.mr.wordcount.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author ravikumarvish
 *
 */
public class WordPairCompartor extends WritableComparator {
	
	public WordPairCompartor() {
		super(WordCountPair.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		WordCountPair p1 = (WordCountPair) a;
		WordCountPair p2 = (WordCountPair) b;
		return p1.word.compareTo(p2.word);
	}

}
