package com.jike.Totalsort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(Driver.class);

		job.setMapperClass(TotalsortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(TotalsortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		//璁剧疆鑷畾涔夊垎鍖�
		job.setNumReduceTasks(3);
		job.setPartitionerClass(TotalsortPartitioner.class);
		
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/totalsort.txt"));
		//6.锟斤拷锟斤拷锟斤拷锟轿伙拷锟�
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/totalResult"));
		//7.锟斤拷锟斤拷锟斤拷业
		if(!job.waitForCompletion(true)){
			return;
		}
	}
}
