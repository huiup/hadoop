package com.jike.FileSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(Driver.class);

		job.setMapperClass(SplitMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
//
//		job.setReducerClass(SplitReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/words.txt"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/result"));

		if(!job.waitForCompletion(true)){
			return;
		}
	}
}
