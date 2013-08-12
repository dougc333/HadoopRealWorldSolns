package com.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//hard to fail, just long arrays going across the network
//text compression would be bad to get to failure
//
public class FailMapReduceBigArray extends Configured implements Tool {

	static class FailMapReduceBigArrayMapper extends
			Mapper<Object, Text, Text, Text[]> {

		public void map(Object obj, Text text, Context context)
				throws IOException, InterruptedException {
			Text arr[] = new Text[1000];
			for (int i = 0; i < 1000; i++) {
				Text bigText = new Text();
				bigText.append("asdf".getBytes(), 0, "asdf".length());
				arr[i] = bigText;
			}

			Text t = new Text();
			t.set("asdf");
			context.write(t, arr);
		}
	}

	static class FailMapReduceBigArrayReducer extends
			Reducer<Text, LongWritable[], IntWritable, Text> {

		public void reduce(Text text, Text[] textArr, Context context)
				throws IOException, InterruptedException {
			for (int i = 0; i < textArr.length; i++) {
				context.write(new IntWritable(i), textArr[i]);
			}
		}

	}

	public static void main(String args[]) throws Exception {
		int returnCode = ToolRunner.run(new FailMapReduceBigArray(), args);
		System.out.println(returnCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		// this is the old api?
		Job job = new Job(conf);
		job.setJarByClass(FailMapReduceBigArray.class);
		job.setMapOutputKeyClass(Text.class);
		// array replace w/ArrayWritable
		job.setMapOutputValueClass(LongWritable.class);
		job.setMapperClass(FailMapReduceBigArrayMapper.class);

		job.setReducerClass(FailMapReduceBigArrayReducer.class);
		int returnCode = job.waitForCompletion(true) ? 0 : 1;
		return returnCode;
	}
}
