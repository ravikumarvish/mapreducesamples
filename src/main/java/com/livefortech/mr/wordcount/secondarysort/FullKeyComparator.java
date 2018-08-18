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
public class FullKeyComparator extends WritableComparator {

	public FullKeyComparator() {
		super(WordCountPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {

		WordCountPair key1 = (WordCountPair) wc1;
		WordCountPair key2 = (WordCountPair) wc2;

		int wordCmp = key1.word.toLowerCase().compareTo(key2.word.toLowerCase());
		if (wordCmp != 0) {
			return wordCmp;
		} else {
			int countCmp = key1.count.compareTo(key2.count);
			if (countCmp != 0) {
				return countCmp;
			} else {
				return -1 * Integer.compare(key1.count, key2.count);
			}
		}

	}

}
