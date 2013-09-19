package com.freitas.hadoop;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopRunner {
	
	private static final Logger log = LoggerFactory.getLogger(HadoopRunner.class);
	
	@SuppressWarnings("rawtypes")
	public void dynamicJob(Map<String, String> map) throws Exception {
		
		String path = map.get("hadoop.conf.dir");
		Configuration conf = new Configuration();
		Path hadoopConfig = new Path(path + "/conf/core-site.xml");
		conf.addResource(hadoopConfig);
		conf.addResource(new Path(path + "/conf/hdfs-site.xml"));
		conf.addResource(new Path(path + "/conf/mapred-site.xml"));
		
		// need to include the JAR with code to send to data nodes
		String sourceJar = map.get("source.jar");
		File jarFile = new File(sourceJar);
		sourceJar = jarFile.toURI().toURL().toExternalForm();
		conf.set("mapred.jar", sourceJar);

		// This is where you can dynamically submit any job you want
		// by changing your input strings.  Of cource, this can get 
		// very complex with a series if-then-elses to check for different 
		// params but this should give you the idea.
		Job job = new Job(conf, map.get("job.name"));

		FileInputFormat.addInputPath(job, new Path(map.get("input.dir")));
		FileOutputFormat.setOutputPath(job, new Path(map.get("output.dir")));

		Class<Mapper> mapper = RunUtilities.tryToLoadClass(sourceJar, map.get("mapper.class"));
		job.setMapperClass(mapper);

		Class<Reducer> reducer = RunUtilities.tryToLoadClass(sourceJar, map.get("reducer.class"));
		job.setReducerClass(reducer);

		Class<Class> outputKey = RunUtilities.tryToLoadClass(sourceJar, map.get("output.key.class"));
		job.setOutputKeyClass(outputKey);

		Class<Class> outputValue = RunUtilities.tryToLoadClass(sourceJar, map.get("output.value.class"));
		job.setOutputValueClass(outputValue);
		
		// Submit the job
		try {
			job.submit();
		} catch (ClassNotFoundException e) {
			log.error("ClassNotFoundException encountered", e);
		} catch (InterruptedException e) {
			log.error("InterruptedException encountered", e);
		}

	}

}
