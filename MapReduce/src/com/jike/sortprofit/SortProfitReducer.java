package com.jike.sortprofit;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

//<单词，数量组成的数组当中元素的类型，单词，总数量>
public class SortProfitReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> value,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		//定义一个count变量，用来做记录总数
		int sum = 0;
		for(IntWritable values:value){
			sum=sum+values.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
