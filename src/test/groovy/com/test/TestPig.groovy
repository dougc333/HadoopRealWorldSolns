package com.test;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


class TestPig {

	static testCh03TSV(){
		String result = println "hadoop fs -stat /user/dc/apache_clf.txt | grep No".execute().text
		if (result == null){
			println "hadoop fs -copyFromLocal '/home/dc/tmppig/apache.clf.txt' '/user/dc'".execute().text
		}

		"hadoop jar /home/dc/workspace/HadoopRealWorldSolns.jar com.test.TestPigTSV /user/dc/apache_clf.txt /user/dc/apache_clf_tsv".execute().text

		//verify /user/dc/apache_clf_tsv exists
		String correct = println "hadoop fs -stat /user/dc/apache_clf_tsv }| grep No".execute().text

		if(!correct){
			println("TSV test correct")
		}else{
			println("TSV test failed")
		}
	}

	//works from command line cant get the pigscript suc
	static testCh03Filter(){

		//remove output directory first


		String isBlackListInHDFS = println "hadoop fs -stat /user/dc/blacklist.txt | grep No".execute().text
		if(!isBlackListInHDFS){
			println "hadoop fs -copyFromLocal /home/dc/bigtop-0.6.0/dl/tmppig/blacklist.txt /user/dc".execute().text
		}

		File pigScript = new File("/home/dc/bigtop-0.6.0/dl/tmppig/pigscript")
		if(pigScript.exists()){
			pigScript.delete()
		}
		pigScript<<"set mapred.cache.files '/user/dc/blacklist.txt';\n"
		pigScript<<"set mapred.create.symlink 'yes';\n"
		pigScript<<"register HadoopRealWorldSolns.jar;\n"
		pigScript<<"\n"
		pigScript<<"\n"
		pigScript<<"all_weblogs = LOAD '/user/dc/apache_nobots_tsv.txt' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);\n"
		pigScript<<"nobots_weblogs = FILTER all_weblogs BY NOT com.test.IsUseragentBot(useragent);\n"
		pigScript<<"\n"
		pigScript<<"\n"
		pigScript<<"STORE nobots_weblogs INTO '/user/dc/nobots_weblogs';"
		pigScript<<"\n"

		println "pig pigscript".execute().text

		//verify
		boolean success = println "hadoop fs -stat /user/dc/nobots_weblogs | grep No".execute().text
		println success

		//clean up
		if(success){
			pigScript.delete();
		}
	}

	//error in book code, nobots->nobots_weblogs
	static Ch03Sort(){

		"hadoop fs -rm -r /user/dc/ordered_weblogs".execute().text

		File pigScript = new File("/home/dc/bigtop-0.6.0/dl/tmppig/pigscript")
		if(pigScript.exists()){
			pigScript.delete()
		}

		"hadoop fs -rm -r /user/dc/ordered_weblogs".execute().text

		pigScript<<"nobots_weblogs = LOAD '/user/dc/apache_nobots_tsv.txt' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);\n"
		pigScript<<"ordered_weblogs = ORDER nobots_weblogs BY timestamp;\n"
		pigScript<<"STORE ordered_weblogs INTO '/user/dc/ordered_weblogs';\n";
		pigScript<<"\n"

		println "pig -f /home/dc/bigtop-0.6.0/dl/tmppig/pigscript".execute().text

		boolean success = "hadoop fs -stat /user/dc/ordered_weblogs | grep No".execute().text
		println success

		if(success){
			"hadoop fs -rm -r /user/dc/ordered_weblogs".execute().text
		}

	}

	//the sessionize function exists in DataFu, lets load that instead of using the book example
	//which seems to be copied from datafu
	static Ch03Sessionize(){
		try{
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/conf/hadoop/core-site.xml"));
			FileSystem fs = FileSystem.get(conf);
			//use the datafu functions already in bigtop
			File f = new File("pigscriptsessionize")
			if(f.exists()){
				f.delete();
			}
			f<<"register datafu-0.6.0.jar;\n";
			f<<"%declare TIME_WINDOW 30m\n"
			f<<"define Sessionize datafu.pig.sessions.Sessionize(TIME_WINDOW)\n"
			f<<"nw = LOAD '/user/dc/apache_nobots_tsv.txt' AS (ip:chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);\n"
			f<<"ip_groups = GROUP np by ip;\n"
			f<<"DUMP ip_groups;\n"
			"pig -f pigscriptsessionize".execute().text;


		}catch(Exception e){
			e.printStackTrace();
		}
	}

	//skip
	static Ch03Python(){

	}

	//run the hadoop command line on TestMR.jar
	static Ch03MRSecondarySort(){

	}

	static Ch03HivePython(){

	}

	static Ch03PythonHadoopStreaming(){

	}

	static Ch03MultipleOutputs(){

	}

	static Ch03WritableGeo(){

	}



	static main(args) {
		//testCh03TSV()
		//testCh03Filter()
		//Ch03Sort()
		Ch03Sessionize()
	}

}
