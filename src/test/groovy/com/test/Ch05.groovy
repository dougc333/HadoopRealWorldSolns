package com.test

import org.apache.hadoop.hbase.*

class Ch05 {

	//didnt we do this already???
	//writing map reduce programs is not the correct strategy for integration tests, we should do this ALL in pig..
	static Join(){

	}

	//this is pointless
	static PigReplicatedMergeSkewedJoin(){

		File f = new File("/user/dc/pigjoin1.pig")
		if(f.exists()){
			f.delete()
		}

		f<<"nobots_weblogs = LOAD '/user/dc/joins/apache_nobots_tsv.txt' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);"
		f<<"ip_country_tbl = LOAD '/user/dc/joins/nobots_ip_country_tsv.txt' AS (ip:chararray, country:chararray);"
		f<<"weblog_country_replicated = JOIN nobots_weblogs BY ip, ip_country_tbl BY ip USING 'replicated';"
		f<<"weblog_country_merge = JOIN nobots_weblogs BY ip, ip_country_tbl BY ip USING 'merge';"
		f<<"weblog_country_skewed = JOIN nobots_weblogs BY ip, ip_country_tbl BY ip USING 'skewed';"

		f<<"store weblog_country_replicated into '/user/dc/joins/weblog_country_replicated';"
		f<<"store weblog_country_merge into '/user/dc/joins/weblog_country_merge';"
		f<<"store weblog_country_skewed into '/user/dc/joins/weblog_country_skewed';"

		println "pig -f /user/dc/pigjoin1.pig".execute().text

		//what is the point of these 3 joins?


	}

	//hive has a MapJoin operator
	//in memory hash join assume this is map side join
	//sort merge bucket join, when tables are bucketed on same key?
	static HiveMapSideJoin(){
	}

	static HiveFullOuterJoins(){
		File hiveOuter = new File("/user/dc/hivefullouter.sql")
		if(hiveOuter.exists()){
			hiveOuter.delete()
		}
		hiveOuter<<"DROP TABLE IF EXISTS acled_nigeria_event_people_links;\n"
		hiveOuter<<"CREATE TABLE acled_nigeria_event_people_links AS SELECT acled.event_date, acled.event_type, vips.name, vips.description as pers_desc, vips.birthday FROM nigeria_vips vips FULL OUTER JOIN acled_nigeria_cleaned acled ON (substr(acled.event_date,6) = substr(vips.birthday, 6));\n"
		hiveOuter<<"SELECT * FROM acled_nigeria_event_people_links WHERE event_date IS NOT NULL AND birthday IS NOT NULL limit 2;\n"

		//modify to count(*)

		"hive -f hivefullouter.sql".execute().text


	}

	//ignore redis. put this material in storm instead. Wont see this in an integration test
	//redis to do joins when you are out of memory but it doesnt scale to cluster size anyway

	//make big nigeria file
	static nigeriabig(){
		File f = new File("")


	}


	//need hundreds of millions of rows where hive fails in a join, we can insert into a single row schema also like hbase
	//join is not the use case, read performacne on 100s million row table
	static HBase(){
		//create t1, f1
		//put t1, r1, f1, v1
		//dont need to add the r1 or v1 in the create statement, rows are sorted on write
		//values are stored in k/v pairs? what is the cell? just the value?
		//
		File f = new File("/home/dc/test.hbase")
		if(f.exists()){
			f.delete()
		}
		//f<<"create 't2','field1';"
		//f<<"insert 't2','row1','field1','a';"
		f<<"list\n"

		String fResult = "hbase shell /home/dc/test.hbase".execute().text
		//you dont see this b/c it forks off another process and you can't get that output
		//does
		println fResult
		//		Configuration hBaseConfig = new org.apache.hadoop.HBase.HBaseConfiguration.create();
		//HTable hTable = new HTable(hBaseConfig, "t1");
		//Scan s = new Scan();


		//		HRegionLocation hLoc = hTable.getRegionLocation("r1");
		//		System.out.println(hLoc.toString());
	}

	//in unit tests
	static PigHBase(){

	}

	static HiveHBase(){


	}



	static main(args) {
		//Join()
		//PigReplicatedMergeSkewedJoin();
		HBase()
	}
}
