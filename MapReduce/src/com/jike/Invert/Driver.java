package com.jike.Invert;


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

		job.setMapperClass(InvertMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(InvertReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/invert"));

		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/invert/result"));

		if(!job.waitForCompletion(true)){

			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(Driver.class);

			job2.setMapperClass(InvertMapper2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setReducerClass(InvertReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/test"));
			FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/test2"));
			
			job.waitForCompletion(true);
			
		}
	}
}
