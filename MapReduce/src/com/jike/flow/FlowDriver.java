package com.jike.flow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FlowDriver {

	public static void main(String[] args) throws Exception {
		//1.����һ����ҵ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.������ҵ�����
		job.setJarByClass(FlowDriver.class);
		//3.����mapper
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		//4.����reducer
		job.setReducerClass(FlowReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		//5.��������λ��
		//设置自定义分区
		job.setNumReduceTasks(3);
		job.setPartitionerClass(FlowPartitioner.class);
		
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/flow.txt"));
		//6.�������λ��
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/Flow"));
		//7.������ҵ
		if(!job.waitForCompletion(true)){
			return;
		}
	}
}
