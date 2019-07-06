package com.jike.shop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class ShopPartitioner extends Partitioner<Text, ShopBean>{

	@Override
	public int getPartition(Text key, ShopBean value, int numreducertask) {
		
		if(value.getName().equals("tianmao")){
			return 0;
		}
		if(value.getName().equals("jingdong")){
			return 1;
		}else{
			return 2;
		}
	}

}
