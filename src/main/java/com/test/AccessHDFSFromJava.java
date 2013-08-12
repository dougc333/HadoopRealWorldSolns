package com.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


public class AccessHDFSFromJava {
		
		public static void main(String []args){
			try{
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			FileSystem fs = FileSystem.get(conf);

			
			RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("/user/dc"), false);
			
			while(it.hasNext()){
				LocatedFileStatus locatedFile = it.next();
				System.out.println(locatedFile.getPath());
			}
		
			
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	

}
