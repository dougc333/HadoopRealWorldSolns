package com.test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

public class DataRecord {
	String ip;
	Timestamp timeStamp;
	String webpageURL;
	Integer statusCode;
	Double randomDoubles[];
	Long randomDoubles2[];
	// change this parameter to 2k, 4k, etc.. till memory fails
	private final int ARRAY_NUM = 20000000;

	public DataRecord() {
		randomDoubles2 = new Long[ARRAY_NUM];
		Random rand = new Random();
		for (int i = 0; i < ARRAY_NUM; i++) {
			randomDoubles2[i] = (long) rand.nextDouble();
		}
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Timestamp getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Timestamp timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getWebpageURL() {
		return webpageURL;
	}

	public void setWebpageURL(String webpageURL) {
		this.webpageURL = webpageURL;
	}

	public Integer getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(Integer statusCode) {
		this.statusCode = statusCode;
	}

	public void parseFileLine(String s) {
		String[] fields = s.split("\t");
		this.setIp(fields[0]);
		this.setTimeStamp(new Timestamp(new Long(fields[1])));
		this.setStatusCode(new Integer(fields[2]));
		this.setWebpageURL(fields[3]);
		ArrayList<Double> alist = new ArrayList<Double>();
		for (int i = 4; i < fields.length; i++) {
			alist.add(Double.parseDouble(fields[i]));
		}
		randomDoubles = new Double[fields.length - 4];
		alist.toArray(randomDoubles);
	}

	public String toString() {
		return ip + "," + timeStamp.toString() + "," + webpageURL + ","
				+ statusCode + ","
				+ "some really big double array which we dont print out";
	}

}
