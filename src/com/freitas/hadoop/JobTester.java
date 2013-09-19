package com.freitas.hadoop;

import java.util.HashMap;
import java.util.Map;


public class JobTester {
	
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("Start test");
		
		// create a map with all of your job parameters
		Map<String, String> map = new HashMap<String, String>();
		map.put("hadoop.conf.dir", "/etc/hadoop");
		map.put("source.jar", "lib/hadoop-examples-2.0.0-mr1-cdh4.3.0.jar");
		map.put("job.name", "Dynamic WordCount");
		map.put("input.dir", "word-count-in");
		map.put("output.dir", "word-count-out");
		map.put("mapper.class", "org.apache.hadoop.examples.WordCount$TokenizerMapper");
		map.put("reducer.class", "org.apache.hadoop.examples.WordCount$IntSumReducer");
		map.put("output.key.class", "org.apache.hadoop.io.Text");
		map.put("output.value.class", "org.apache.hadoop.io.IntWritable");
		
		HadoopRunner tester = new HadoopRunner();
		tester.dynamicJob(map);
		
		System.out.println("End test");
	}
	

}
