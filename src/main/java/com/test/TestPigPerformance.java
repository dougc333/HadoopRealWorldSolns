package com.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//step 1: show failure in local memory running in eclipse when changing records from 1k to 2k
//step 2: show failure in pig when loading testpigperf in grunt shell
//step 3: create separate m/r program showing failed joins when you get more data in memory
public class TestPigPerformance extends Configured implements Tool {
	static final String FILE_LOC = "/home/dc/testpigperf.txt";
	static Integer numMappers;

	public TestPigPerformance() {
	}

	// 1000 DR records, 1gb
	// 2000 DR records, out of memory, show this runs on m/r, each mapper should
	// be in a different JVM
	private void TestStandalone() throws IOException {
		createData();
		parseFile();
	}

	static String[] someTimestamps = { "1341391325000", "1341365098000",
			"1341376657000", "1341388286000" };
	static final int NUM_RECORDS = 1000000;
	static final String SEP = "\t";

	private static void createData() throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(FILE_LOC));
		for (int i = 0; i < NUM_RECORDS; i++) {
			bw.write("0.0.0.0" + SEP + someTimestamps[0] + SEP + "100" + SEP
					+ "http://www.google.com" + SEP
					+ createDoubles().toString());
			bw.newLine();
			bw.write("0.0.0.1" + SEP + someTimestamps[1] + SEP + "200" + SEP
					+ "http://www.cnn.com" + SEP + createDoubles().toString());
			bw.newLine();
			bw.write("0.0.0.2" + SEP + someTimestamps[2] + SEP + "300" + SEP
					+ "http://www.asdf.com" + SEP + createDoubles().toString());
			bw.newLine();

			bw.write("0.0.0.3" + SEP + someTimestamps[3] + SEP + "400" + SEP
					+ "http://www.bbbb.com" + SEP + createDoubles().toString());
			bw.newLine();
		}

		bw.close();
	}

	// this will take forever to run b/c it writes to disk in above funciton
	// better to modify in DataRecord and only allocate to memory
	// leave this function here for documentation
	private static StringBuilder createDoubles() {
		StringBuilder sb = new StringBuilder();
		Random rand = new Random();
		for (int i = 0; i < 10; i++) {
			sb.append(rand.nextDouble() + SEP);
		}
		return sb;

	}

	private static void parseFile() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(FILE_LOC));
			String fileLine = null;
			Integer num = 0;
			System.out.println("memory before dr created");
			System.gc();
			System.runFinalization();
			long totalMemory = Runtime.getRuntime().totalMemory();
			long freeMemory = Runtime.getRuntime().freeMemory();
			System.out.println("used memory before test:"
					+ (totalMemory - freeMemory) / (1024 * 1024));
			System.out.println("before DR creation total memory:"
					+ ((totalMemory - freeMemory) / (1024 * 1024)));
			long startTime = System.currentTimeMillis();
			ArrayList<DataRecord> list = new ArrayList<DataRecord>();
			DataRecord dr = new DataRecord();
			while ((fileLine = br.readLine()) != null) {
				dr.parseFileLine(fileLine);
				list.add(dr);
				num++;
			}
			long endTime = System.currentTimeMillis();
			System.gc();
			System.runFinalization();
			long afterMemory = Runtime.getRuntime().totalMemory();
			long afterFreeMemory = Runtime.getRuntime().freeMemory();
			System.out.println("num datarecords:" + num);
			System.out.println("mem used:" + (afterMemory - afterFreeMemory)
					/ (1024 * 1024) + "MBs");
			System.out.println("elapese time:" + (endTime - startTime) / 1000
					+ "seconds");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** map reduce section */

	static class TestPigPerfMapper extends Mapper<Object, Text, Text, Text> {

		void setup() {
			numMappers++;
			System.out.println(numMappers);
		}

		// print out num mappers
		protected void map(Object obj, Text text, Context context)
				throws IOException, InterruptedException {
			System.out.println("input split:" + text.toString());
			String[] fields = text.toString().split("\t");
			// we can make the dr big enough to run out of memory in the string
			// part
			DataRecord dr = new DataRecord();
			// oops need a serialization format
			dr.parseFileLine(text.toString());
			Text t = new Text();
			t.set("a");
			Text t1 = new Text();
			t1.set(dr.toString());
			// show mapper failure when we run out of memory
			context.write(t, t1);
		}
	}

	static class TestPigPerfReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		void reduce(Text text, Text dr, Context context) {
			System.out.println("text:" + text.toString());
			System.out.println("textdr:" + dr.toString());

		}
	}

	public static void main(String[] args) {
		try {
			// first show failure in creating objects in memory
			// new TestPigPerformance() {
			// {
			// createData();
			// parseFile();
			// }
			// };

			// second create file from above, load into pig and show failure in
			// pig load
			int returnCode = ToolRunner.run(new TestPigPerformance(), args);
			System.out.println(returnCode);
			// third show pig failure in join

			// System.out.println("asfd");
			// int returnCode = ToolRunner.run(new TestPigPerformance(), args);
			// System.out.println(returnCode);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		// FileSystem fs = FileSystem.get(conf);
		// add test if exists using Java file system
		// File f = new File("/home/dc/testpigperf.txt");
		// if (f.exists()) {
		// f.delete();
		// }

		// createData();

		// if (!fs.exists(new Path("pigperfdata"))) {
		// fs.create(new Path("pigperfdata"));
		// Path[] paths = new Path[1];
		// paths[0] = new Path("/home/dc/testpigperf.txt");
		// fs.copyFromLocalFile(true, true, paths, new Path(
		// "/user/dc/pigperfdata"));
		// }

		// copy to HDFS if not exist

		Job job = new Job();
		job.setJarByClass(TestPigPerformance.class);
		job.setMapperClass(TestPigPerfMapper.class);
		job.setMapOutputKeyClass(Text.class); // what is the key? IPVALUE?
		job.setMapOutputValueClass(Text.class);
		// test one list in one single reducer; see this fail...
		// test one list in each reducer...
		job.setReducerClass(TestPigPerfReducer.class);
		// job.setNumReduceTasks(4); this should match the number of mappers
		// why dont the reducers have setReducerValue,Key classes?
		FileInputFormat.setInputPaths(job, new Path("/user/dc/pigperfdata"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/dc/pigperfdata/output"));

		int returnCode = job.waitForCompletion(true) ? 0 : 1;
		return returnCode;
	}
}
