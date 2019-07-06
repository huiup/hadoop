package com.jike.guanxi;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GuanxiReducer extends Reducer<Text, Text, Text, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		
		ArrayList<String> grandpaList=new ArrayList<>();
		//--用于存储儿子的数据
		ArrayList<String> grandsonList=new ArrayList<>();
		
		for(Text value:values){
			if(value.toString().startsWith("+")){
				grandpaList.add(value.toString().substring(1));
			}else{
				grandsonList.add(value.toString().substring(1));
			}
		}
		if(grandpaList.size()>0&&grandsonList.size()>0){
			String grandpa=grandpaList.toString();
			String grandson=grandsonList.toString();
			
			String relation="爷爷辈:"+grandpa+"-->孙子辈:"+grandson;
			
			context.write(new Text(relation),NullWritable.get());
		}
//		ArrayList<String> ye = new ArrayList<>();
//		ArrayList<String> sun = new ArrayList<>();
//		for (Text res : value) {
//			if (res.toString().startsWith("+")) {
//				ye.add(res.toString().substring(1));
//			}else{
//				sun.add(res.toString().substring(1));
//			}
//		}
//		if(ye.size()>0&&sun.size()>0){
//			String s1 = ye.toString();
//			String s2 = sun.toString();
//			
//			String rel = "爷爷辈"+s1+"---孙子辈"+s2;
//			context.write(new Text(rel),NullWritable.get());
//
//		}


	}

}
