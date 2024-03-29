package com.jike.shop;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShopMapper extends Mapper<LongWritable, Text, Text, ShopBean> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ShopBean>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] arr = line.split(" ");
		ShopBean fb = new ShopBean(Integer.parseInt(arr[0]),arr[1],Integer.parseInt(arr[2]));
		
		String month = arr[0];
		context.write(new Text(month), fb);
	}
}
