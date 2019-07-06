package com.jike.characters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class CharDriver {

	public static void main(String[] args) throws Exception {
		//1.����һ����ҵ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.������ҵ�����
		job.setJarByClass(CharDriver.class);
		//3.����mapper
		job.setMapperClass(CharMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//4.����reducer
		job.setReducerClass(CharReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//5.��������λ��
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/characters.txt"));
		//6.�������λ��
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/CharCount"));
		//7.������ҵ
		if(!job.waitForCompletion(true)){
			return;
		}
	}
}
