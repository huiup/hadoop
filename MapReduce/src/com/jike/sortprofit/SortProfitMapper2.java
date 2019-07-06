package com.jike.sortprofit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortProfitMapper2 extends Mapper<LongWritable, Text, SortProfit, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, SortProfit, NullWritable>.Context context)
					throws IOException, InterruptedException {
		String line = value.toString();
		//生成的结果文件，数据之间是用制表符隔开的
		String[] arr = line.split("\t");
		int month=Integer.parseInt(arr[0]);
		int profit=Integer.parseInt(arr[1]);

		SortProfit sf = new SortProfit();
		sf.setMonth(month);
		sf.setProfit(profit);
		context.write(sf, NullWritable.get());
		
	}

}