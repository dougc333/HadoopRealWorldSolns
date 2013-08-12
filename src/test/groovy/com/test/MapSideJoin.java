package com.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoin extends Configured implements Tool {

	static class WeblogMapper extends Mapper<Object, Text, Text, Text> {

		public static final String IP_COUNTRY_TABLE_FILENAME = "nobots_ip_country_tsv.txt";
		private Map<String, String> ipCountryMap = new HashMap<String, String>();

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] files = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path p : files) {
				if (p.getName().equals(IP_COUNTRY_TABLE_FILENAME)) {
					BufferedReader reader = new BufferedReader(new FileReader(
							p.toString()));
					String line = reader.readLine();
					while (line != null) {
						String[] tokens = line.split("\t");
						String ip = tokens[0];
						String country = tokens[1];
						ipCountryMap.put(ip, country);
						line = reader.readLine();
					}
				}
			}

			if (ipCountryMap.isEmpty()) {
				throw new IOException("Unable to load IP country table.");
			}
		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String row = value.toString();
			String[] tokens = row.split("\t");
			String ip = tokens[0];
			String country = ipCountryMap.get(ip);
			outputKey.set(country);
			outputValue.set(row);
			context.write(outputKey, outputValue);
		}

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new MapSideJoin(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		DistributedCache.addCacheFile(new URI(
				"/user/dc/nobots_ip_country_tsv.txt"), conf);
		Job weblogJob = new Job(conf);
		weblogJob.setJobName("MapSideJoin");
		weblogJob.setNumReduceTasks(0);
		weblogJob.setJarByClass(getClass());
		weblogJob.setMapperClass(WeblogMapper.class);
		weblogJob.setMapOutputKeyClass(Text.class);
		weblogJob.setMapOutputValueClass(Text.class);
		weblogJob.setOutputKeyClass(Text.class);
		weblogJob.setOutputValueClass(Text.class);
		// weblogJob.setInputFormatClass(TextInputFormat.class);
		// weblogJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(weblogJob, inputPath);
		FileOutputFormat.setOutputPath(weblogJob, outputPath);

		return weblogJob.waitForCompletion(true) ? 0 : 1;
	}
}
