package com.test;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TestPigTSV extends Configured implements Tool{

	//book is wrong, need static else compiler creates a static ctor from the parent class
	//and there is no default ctor w/no args b/c the parent doesnt have one
	public static class CLFMapper extends Mapper<Object, Text, Text, Text>{

	    private SimpleDateFormat dateFormatter = 
	            new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
	    private Pattern p = 
	            Pattern.compile("^([\\d.]+) (\\S+) (\\S+)"
	            + " \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" "
	            + "(\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"", 
	            Pattern.DOTALL);
	    
	    private Text outputKey = new Text();
	    private Text outputValue = new Text();
	    
	    
	    @Override
	    protected void map(Object key, Text value, Context 
	      context) throws IOException, InterruptedException {
	        String entry = value.toString();
	        Matcher m = p.matcher(entry);
	        if (!m.matches()) {
	            return;
	        }
	        Date date = null;
	        try {
	            date = dateFormatter.parse(m.group(4));
	        } catch (ParseException ex) {
	            return;
	        }
	        outputKey.set(m.group(1)); //ip
	        StringBuilder b = new StringBuilder();
	        b.append(date.getTime()); //timestamp
	        b.append('\t');
	        b.append(m.group(6)); //page
	        b.append('\t');
	        b.append(m.group(8)); //http status
	        b.append('\t');
	        b.append(m.group(9)); //bytes
	        b.append('\t');
	        b.append(m.group(12)); //useragent
	        outputValue.set(b.toString());
	        context.write(outputKey, outputValue);
	    }
	   
	}
	
	public int run(String[] args) throws Exception {
	    
	    Path inputPath = new Path(args[0]);
	    Path outputPath = new Path(args[1]);
	    
	    Configuration conf = getConf();
	    Job weblogJob = new Job(conf);
	    weblogJob.setJobName("Weblog Transformer");
	    weblogJob.setJarByClass(getClass());
	    weblogJob.setNumReduceTasks(0);
	    weblogJob.setMapperClass(CLFMapper.class);        
	    weblogJob.setMapOutputKeyClass(Text.class);
	    weblogJob.setMapOutputValueClass(Text.class);
	    weblogJob.setOutputKeyClass(Text.class);
	    weblogJob.setOutputValueClass(Text.class);
	    weblogJob.setInputFormatClass(TextInputFormat.class);
	    weblogJob.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(weblogJob, inputPath);
	    FileOutputFormat.setOutputPath(weblogJob, outputPath);
	    
	    
	    if(weblogJob.waitForCompletion(true)) {
	      return 0;
	    }
	    return 1;
	  }
	  
	  public static void main( String[] args ) throws Exception {
	    int returnCode = ToolRunner.run(new TestPigTSV(), args);
	    System.exit(returnCode);
	  }	
	
	
}
