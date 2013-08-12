package com.test;

import java.io.IOException;

import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EmployeeRDFTextInputFormat extends
		TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {

	public static void main(String[] args) {

	}

	@Override
	public org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}