package com.freitas.hadoop;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PigRunnerTester {
	
private static final Logger log = LoggerFactory.getLogger(PigRunnerTester.class);
	
	public PigRunnerTester() {
		log.error("Enter PigRunnerTester");
		try {
			File f = new File("/usr/local/hadoop-1.0.4/conf/");
			URL u = f.toURI().toURL();
			URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
			Class<URLClassLoader> urlClass = URLClassLoader.class;
			Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
			method.setAccessible(true);
			method.invoke(urlClassLoader, new Object[]{u});
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e);
		}
		
		log.error("Exit PigRunnerTester");
	}
	
	public void runSimpleScript(String fgScript) throws IOException {
		log.error("Enter runSimpleScript");

		try {
			String[] args = { "-stop_on_failure", fgScript };
			TestNotificationListener listener = new TestNotificationListener();
			PigStats stats = PigRunner.run(args, listener);
			log.error("After job stats here");
			log.error("Return code: " + stats.getReturnCode());
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		log.error("Exit runSimpleScript");
	}
	

}
