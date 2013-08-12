package com.test;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NGramJob extends Configured implements Tool {

	private Configuration conf;

	public static final String NAME = "ngram";
	private static final String GRAM_LENGTH = "number_of_grams";

	static class NGramJobMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private int gram_length;
		private Pattern space_pattern = Pattern.compile("[ ]");
		private StringBuilder gramBuilder = new StringBuilder();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			gram_length = context.getConfiguration().getInt(
					NGramJob.GRAM_LENGTH, 2);
		}

		// should preinstanstiate the Text object
		public void map(Object obj, Text text, Context context)
				throws IOException, InterruptedException {
			String[] fields = space_pattern.split(text.toString());

			for (int i = 0; i < fields.length; i++) {
				//
				gramBuilder.setLength(0);
				if (i + gram_length <= fields.length) {
					for (int j = i; j < i + gram_length; j++) {
						gramBuilder.append(fields[j]);
						gramBuilder.append(" ");
					}
				}
				context.write(new Text(gramBuilder.toString()),
						NullWritable.get());
			}
		}

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new NGramJob(), args));
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		config.addResource("core-site.xml");
		config.addResource("hdfs-site.xml");

		Job job = new Job(config);
		job.setJarByClass(NGramJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(NGramJobMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//
		FileInputFormat.addInputPath(job,
				new Path("/user/dc/hive/newsarchives"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/dc/hive/newsarchives/output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
