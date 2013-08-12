package com.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DataRecordWritable implements Writable {
	Text ip;
	LongWritable timeStamp;
	Text webpageURL;
	IntWritable statusCode;
	LongWritable randomDoubles[];
	// lets try something new
	ArrayListWritable alw;

	static final int ARRAY_NUM = 1000;

	public DataRecordWritable() {
		randomDoubles = new LongWritable[ARRAY_NUM];
		Random rand = new Random();
		for (int i = 0; i < ARRAY_NUM; i++) {
			LongWritable l = new LongWritable();
			l.set((long) rand.nextDouble());
			randomDoubles[i] = l;
		}

		ArrayList<Long> al = new ArrayList<Long>();
		for (int j = 0; j < 1000; j++) {

		}

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {

	}

	@Override
	public void write(DataOutput out) throws IOException {
		ip.write(out);
		timeStamp.write(out);
		webpageURL.write(out);
		statusCode.write(out);

	}

}
