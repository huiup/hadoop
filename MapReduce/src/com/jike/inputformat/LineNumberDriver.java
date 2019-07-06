package com.jike.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class LineNumberDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(LineNumberDriver.class);
		
		job.setMapperClass(LineNumberMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		//设置自定义格式输出组件
		//如果不设置，则用默认的格式组件，即输入key是每行行首偏移量
		//输出value为每行内容
		job.setInputFormatClass(LineNumberFormat.class);;
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/input"));

		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/input/result"));

		job.waitForCompletion(true);
	}
}
