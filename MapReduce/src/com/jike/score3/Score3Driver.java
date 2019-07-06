package com.jike.score3;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Score3Driver {

	public static void main(String[] args) throws Exception {
		//1.����һ����ҵ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.������ҵ�����
		job.setJarByClass(Score3Driver.class);
		//3.����mapper
		job.setMapperClass(Score3Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreBean.class);
		//4.����reducer
		job.setReducerClass(Score3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreBean.class);
		//5.��������λ��
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.157.27:9000/score.txt"));
		//6.�������λ��
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.157.27:9000/score33"));
		//7.������ҵ
		if(!job.waitForCompletion(true)){
			return;
		}
	}
}
