package com.livefortech.mr.wordcount.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class WordCountPair implements Writable, WritableComparable<WordCountPair> {

	public String word = new String();
	public Integer count ;
	
	public WordCountPair() {
		
	}
	
	public WordCountPair(String word, Integer count) {
		this.word = word;
		this.count = count;
	}
	
	@Override
	public int compareTo(WordCountPair o) {
		int wordcomp = this.word.compareTo(o.word);
		if(wordcomp != 0) {
			return wordcomp;
		} else {
			int countComp = this.count.compareTo(o.count);
			return countComp;
		}
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		word = input.readUTF();
		count = input.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(count);
		
	}

	

	

}
