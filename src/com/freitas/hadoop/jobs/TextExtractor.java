package com.freitas.hadoop.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TextExtractor extends Configured implements Tool {


	public static class MapClass extends
			Mapper<NullWritable, BytesWritable, Text, DoubleWritable> {
		
		@Override
		public void setup(Context conf) {
			try {

				// FIXME: add your custom setup stuff here

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			//FIXME: do your custom processing here
			String someKey = "Some key";
			Double someValue = 100.0;
			context.write(new Text(someKey), new DoubleWritable(someValue));
			
		}

	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable v : values) {
				sum += v.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf, "TextExtractor");
		job.setJarByClass(MapClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
		return 0;
	}

}
