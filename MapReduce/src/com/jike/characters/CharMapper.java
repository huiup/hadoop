package com.jike.characters;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//<ƫ�������ı����ݣ����ʣ�����>
public class CharMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//1.��ȡ������	
		String line = value.toString();
		//2.���տո��зֵ���
		char[] arr = line.toCharArray();
		//3.ѭ���������飬������ʺ�����
		for(char word : arr){
//			if(word.equals(" ")){
//				continue;
//			}
			context.write(new Text(word+""), new LongWritable(1));
		}
	}
}

