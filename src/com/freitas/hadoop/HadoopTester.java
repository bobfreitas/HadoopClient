package com.freitas.hadoop;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.freitas.hadoop.jobs.TextExtractor.MapClass;
import com.freitas.hadoop.jobs.TextExtractor.Reduce;
import com.freitas.hadoop.jobs.WholeFileInputFormat;

public class HadoopTester {
	
	String path = "/usr/local/hadoop-1.0.4";
	private String IN_DIR = "/user/someone/extract/in";
	private String OUT_DIR = "/user/someone/extract/out";

	public void cliStyleJob() throws Exception {
		
		Configuration conf = new Configuration();
		Path hadoopConfig = new Path(path + "/conf/core-site.xml");
		conf.addResource(hadoopConfig);
		conf.addResource(new Path(path + "/conf/hdfs-site.xml"));
		conf.addResource(new Path(path + "/conf/mapred-site.xml"));

		Job job = new Job(conf, "TextExtractor");
		job.setJarByClass(MapClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(IN_DIR));
		FileOutputFormat.setOutputPath(job, new Path(OUT_DIR));

		// Submit the job
		try {
			job.submit();
		} catch (ClassNotFoundException e) {
			// FIXME: do something better here
			e.printStackTrace();
		} catch (InterruptedException e) {
			// FIXME: do something better here
			e.printStackTrace();
		}

	}



	@SuppressWarnings("rawtypes")
	public void dynamicJob() throws Exception {
		
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
				"com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

		Configuration conf = new Configuration();
		Path hadoopConfig = new Path(path + "/conf/core-site.xml");
		conf.addResource(hadoopConfig);
		conf.addResource(new Path(path + "/conf/hdfs-site.xml"));
		conf.addResource(new Path(path + "/conf/mapred-site.xml"));
		
		/*
		 * This assumes that the job related classes have been separated into an independent 
		 * jar that can then be submitted with the job (not included in this project)
		 */
		String sourceJar = "/user/someone/lib/JobStuff.jar";
		File jarFile = new File(sourceJar);
		sourceJar = jarFile.toURI().toURL().toExternalForm();
		conf.set("mapred.jar", sourceJar);

		Job job = new Job(conf, "TextExtractor");

		FileInputFormat.addInputPath(job, new Path(IN_DIR));
		FileOutputFormat.setOutputPath(job, new Path(OUT_DIR));

		String mapperClassName = "com.freitas.hadoop.jobs.TextExtractor$MapClass";
		Class<Mapper> mapper = RunUtilities.tryToLoadClass(sourceJar, mapperClassName);
		job.setMapperClass(mapper);

		String reducerClassName = "com.freitas.hadoop.jobs.TextExtractor$Reduce";
		Class<Reducer> reducer = RunUtilities.tryToLoadClass(sourceJar, reducerClassName);
		job.setReducerClass(reducer);

		String combinerClassName = "com.freitas.hadoop.jobs.TextExtractor$Reduce";
		Class<Reducer> combiner = RunUtilities.tryToLoadClass(sourceJar, combinerClassName);
		job.setCombinerClass(combiner);

		String inputFormatClassName = "com.kitenga.entities.hadoop.WholeFileInputFormat";
		Class<InputFormat> inputFormat = RunUtilities.tryToLoadClass(sourceJar, inputFormatClassName);
		job.setInputFormatClass(inputFormat);

		String outputKeyClassName = "org.apache.hadoop.io.Text";
		Class<Class> outputKey = RunUtilities.tryToLoadClass(sourceJar, outputKeyClassName);
		job.setOutputKeyClass(outputKey);

		String outputKeyValueName = "org.apache.hadoop.io.DoubleWritable";
		Class<Class> outputValue = RunUtilities.tryToLoadClass(sourceJar, outputKeyValueName);
		job.setOutputValueClass(outputValue);
		
		// Submit the job
		try {
			job.submit();
		} catch (ClassNotFoundException e) {
			// FIXME: do something better here
			e.printStackTrace();
		} catch (InterruptedException e) {
			// FIXME: do something better here
			e.printStackTrace();
		}

	}

}
