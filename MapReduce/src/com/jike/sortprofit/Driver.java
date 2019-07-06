package com.jike.sortprofit;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//先分区再按key合并
public class Driver {

	public static void main(String[] args) throws Exception {
		//1.声明一个作业
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.声明作业的入口
		job.setJarByClass(Driver.class);
		//3.声明mapper
		job.setMapperClass(SortProfitMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
//		//4.����reducer
		job.setReducerClass(SortProfitReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/sortproifit.txt"));
		//6.�������λ��
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/sortprofitResult"));
		//7.������ҵ
		if(!job.waitForCompletion(true)){
			return;
		}
	}
}
