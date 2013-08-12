package com.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PigUDF extends EvalFunc<Object> {
	private Pattern p = Pattern
			.compile(
					"^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\w+) (.+?) (.+?)\" (\\d+) (\\d+) \"([^\"]+|(.+?))\" \"([^\"]+|(.+?))\"",
					Pattern.DOTALL);
	private SimpleDateFormat dateFormatter = new SimpleDateFormat(
			"dd/MMM/yyyy:HH:mm:ss Z");

	@Override
	public Object exec(Tuple arg0) throws IOException {
		// TODO Auto-generated method stub

		for (Object item : arg0.getAll()) {
			System.out.println(item.toString());
		}
		return arg0;
	}
}
