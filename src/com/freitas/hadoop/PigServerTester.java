package com.freitas.hadoop;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.ScriptState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PigServerTester {
	
private static final Logger log = LoggerFactory.getLogger(PigServerTester.class);
	
	PigServer pigServer = null;

	public PigServerTester() {
		log.error("Enter PigServerTester");
		Properties props = new Properties();
		props.setProperty("fs.default.name", "hdfs://localhost:9000");
		props.setProperty("mapred.job.tracker", "localhost:9001");
		ScriptState.get().registerListener(new TestNotificationListener());
		try {
			
			PigContext pigContext = new PigContext(ExecType.MAPREDUCE, props);
			
			File f = new File("/usr/local/hadoop-1.0.4/conf/");
			URL u = f.toURI().toURL();
			URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
			Class<URLClassLoader> urlClass = URLClassLoader.class;
			Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
			method.setAccessible(true);
			method.invoke(urlClassLoader, new Object[]{u});
			
			pigServer = new PigServer(pigContext);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
		log.error("Exit PigServerTester");

	}
	
	public void runQuerySimple() throws IOException {
		log.error("Enter runQuerySimple");

		pigServer.registerQuery("records = LOAD '/user/someone/pig/weather/input/weather.txt' USING PigStorage(',') AS (year:chararray, temperature:int, quality:int);");
		pigServer.registerQuery("filtered_records = FILTER records BY temperature != 9999 AND (quality == 0 OR quality == 1 OR quality == 4 OR quality == 5 OR quality == 9);");
		pigServer.registerQuery("grouped_records = GROUP filtered_records BY year;");
		pigServer.registerQuery("max_temp = FOREACH grouped_records GENERATE group, MAX(filtered_records.temperature);");

		pigServer.store("max_temp", "/user/someone/pig/weather/output");
		log.error("Exit runQuerySimple");
	}
	
	
	public void runQueryStream(InputStream in) throws IOException {
		log.error("Enter runQueryStream");
		pigServer.registerScript(in);
		log.error("Exit runQueryStream");
	}
	
	
	public void runQueryStreamBatch(InputStream in) throws IOException {
		log.error("Enter runQueryStreamBatch");
		pigServer.setBatchOn();
		pigServer.registerScript(in);
		List<ExecJob> status = pigServer.executeBatch();
		
		System.out.println("Number of jobs: " + status.size());
		
		log.error("Exit runQueryStreamBatch");
		
	}

}
