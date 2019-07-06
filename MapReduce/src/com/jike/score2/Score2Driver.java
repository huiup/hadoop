package com.jike.score2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Score2Driver {

	public static void main(String[] args) throws Exception {
		//1.声明一个作业
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.声明作业的入口
		job.setJarByClass(Score2Driver.class);
		//3.声明mapper
		job.setMapperClass(Score2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//4.声明reducer
		job.setReducerClass(Score2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//5.声明输入位置
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/score2"));
		
		//6.声明输出位置
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/score2Res"));
		//7.启动作业
		if(!job.waitForCompletion(true)){
			return;
		}
	}
}