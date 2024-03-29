package com.jike.scoreInput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScoreDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(ScoreDriver.class);
		job.setMapperClass(ScoreMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//--设定自义定的记录处理组价
		//--如果不设定，默认用的是TextInputFormat
		job.setInputFormatClass(ScoreInputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.157.27:9000/input"));
		
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/input/result"));
		
		job.waitForCompletion(true);
	}
}
