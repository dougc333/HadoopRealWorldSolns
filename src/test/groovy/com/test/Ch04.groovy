package com.test

class Ch04 {

	static HiveTable(){

		//create file hive1.sql

		File hiveSQLFile = new File("/home/dc/hive1.sql")
		if(hiveSQLFile.exists()){
			hiveSQLFile.delete()
		}
		hiveSQLFile<<"DROP TABLE IF EXISTS weblog_entries;"
		hiveSQLFile<<"create external table weblog_entries(md5 STRING, url string, request_date string, request_time string, ip string) row format delimited fields terminated by '\t' lines terminated by '\n' location '/user/dc/hive/weblog/';"
		hiveSQLFile<<"select count(*) from weblog_entries;"

		String foo =  "wc /home/dc/Downloads/weblog_entries.txt".execute().text
		List splitFoo = foo.tokenize();
		String numRows = println splitFoo[0]

		String numRowsTable =  "hive -f /home/dc/hive1.sql".execute().text
		if(numRows.equals(numRowsTable)){
			print "num rows in hive table match num rows in file"
		}else{
			print "test failed"
		}
	}

	static HiveDynamicTable(){
		File hiveSQLFile2 = new File("/home/dc/hive2.sql");
		if(hiveSQLFile2.exists()){
			hiveSQLFile2.delete()
		}
		hiveSQLFile2<<"DROP TABLE IF EXISTS weblog_entries2;"
		hiveSQLFile2<<"create table weblog_entries2 as select url,request_date, request_time, length(url) as url_length from weblog_entries;"
		hiveSQLFile2<<"select count(*) from  weblog_entries2;"

		println "hive -f /home/dc/hive2.sql".execute().text
	}

	static HiveUDFs(){
		File hiveSQLFile = new File('/home/dc/hive3.sql');
		if(hiveSQLFile.exists()){
			hiveSQLFile.delete()
		}
		hiveSQLFile<<"DROP TABLE IF EXISTS weblog_entries3;"
		hiveSQLFile<<"create table weblog_entries3 as select concat_ws('_', request_date, request_time) from weblog_entries;"
		hiveSQLFile<<"select count(*) from weblog_entries3;"
		println "hive -f /home/dc/hive3.sql".execute().text

	}

	static HiveIntersect(){
		File hiveSQLFile = new File('/home/dc/hive4.sql');
		if(hiveSQLFile.exists()){
			hiveSQLFile.delete()
		}
		hiveSQLFile<<"DROP TABLE IF EXISTS ip_to_country;"
		hiveSQLFile<<"create external table ip_to_country(ip string, country string) row format delimited fields terminated by '\t' lines terminated by '\n' location '/user/dc/hive/iptocountry';"
		//hiveSQLFile<<"select count(*) from ip_to_country;"

		//intersect, what happens when both tables dont have the same # of rows?
		hiveSQLFile<<"select count(itc.country) from weblog_entries wl JOIN ip_to_country itc on wl.ip=itc.ip;"
		//this becomes a 2 stage map reduce job. There is no direcct output here..
		//need the foo var to get the 3000 output...
		String foo= "hive -f /home/dc/hive4.sql".execute().text
		println foo

	}


	//dont spend more time on the MR code, this will never make it as an integration test
	//too low level. This is nuts, convert this to python instead
	static Ngrams(){

		//load data into hdfs
		String dataloaded = "hadoop fs -stat '/user/dc/hive/newsarchives/news' | grep 'No'".execute().text
		if(dataloaded!=null){
			"hadoop fs -rm /user/dc/hive/newsarchives/news".execute().text
		}else{
			print "import newsarchives into hdfs"
		}


		//if exists, delete

		"hadoop fs -rm -r '/user/dc/hive/newsarchives/output'".execute().text

		"hadoop jar HadoopRealWorldSolns.jar com.test.NGramJob".execute().text
		"hadoop fs -cat /user/dc/hive/newsarchives/output/part-r-00000".execute.text
	}

	//MR code, covert this to python instead. Is this a good idea? Python MR code for integration tests?
	static DistCache(){
	}



	static Pig(){

		"hadoop fs -rm -r /user/dc/hivedata/ipcountries/output".execute().text

		File pigFile = new File("/user/dc/pig1.pig")
		if(pigFile.exists()){
			pigFile.delete()
		}


		pigFile<<"ip_countries = LOAD '/user/dc/hivedata/ipcountries/ip_to_country.txt' as (ip:chararray, country:chararray);"
		pigFile<<"country_grid = GROUP ip_countries by country;"
		pigFile<<"country_counts = FOREACH country_grid GENERATE FLATTEN(group), COUNT(ip_countries) as counts;"
		pigFile<<"store country_counts into '/user/dc/hivedata/ipcountries/output'"

		"pig -f /user/dc/pig1.pig".execute().text
		//check /user/dc/hivedata/ipcountries/output exists,
	}



	static cleanup(){
		File hiveFile1 = new File("/user/dc/hive1.sql")
		if(hiveFile1.exists()){
			hiveFile1.delete()
		}
		File hiveFile2 = new File("/user/dc/hive2.sql")
		if(hiveFile2.exists()){
			hiveFile2.delete()
		}
		File hiveFile3 = new File("/user/dc/hive3.sql")
		if(hiveFile3.exists()){
			hiveFile3.delete()
		}
		File hiveFile4 = new File("/user/dc/hive4.sql")
		if(hiveFile4.exists()){
			hiveFile4.delete()
		}
		File hiveFile5 = new File("/user/dc/hive5.sql")
		if(hiveFile5.exists()){
			hiveFile5.delete()
		}
		File hiveFile6 = new File("/user/dc/hive6.sql")
		if(hiveFile6.exists()){
			hiveFile6.delete()
		}

	}

	static main(args) {
		//HiveTable()
		//HiveDynamicTable()
		//HiveUDFs()
		//HiveIntersect()
		//Ngrams()
		//DistCache()
		//Pig()
	}
}
