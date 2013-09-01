package com.freitas.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTester {
	
private static final Logger log = LoggerFactory.getLogger(JobTester.class);
	
	public static void main(String[] args) throws Exception {
		log.error("Start test");
		
		boolean pigRunner = true;
		if (pigRunner){
			PigRunnerTester tester = new PigRunnerTester();
			String relScript = "resources" + File.separator + "pig" + File.separator + "maxWeather.pig";
			File relScriptFile = new File(relScript);
			String absFilePath = relScriptFile.getAbsolutePath();
			tester.runSimpleScript(absFilePath);
		}
		
		boolean pigServer = false;
		if (pigServer){
			PigServerTester tester = new PigServerTester();
			String script = "resources" + File.separator + "pig" + File.separator + "maxWeather.pig";
			InputStream in = new FileInputStream(script);
			tester.runQueryStream(in);
		}
		
		boolean cliStyle = false;
		if (cliStyle){
			HadoopTester tester = new HadoopTester();
			tester.cliStyleJob();
		}
		boolean dynamicJob = false;
		if (dynamicJob) {
			HadoopTester tester = new HadoopTester();
			tester.dynamicJob();
		}
		
		log.error("End test");
	}
	

}
