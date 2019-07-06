package com.jike.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/*
 * 
 * key 是mapper 的输出key
 * value是mapper 的输出value
 * 
 * */

public class FlowPartitioner extends Partitioner<Text, FlowBean>{

	@Override
	public int getPartition(Text key, FlowBean value, int numreducertask) {
		
		if(value.getAddr().equals("bj")){
			return 0;
		}
		if(value.getAddr().equals("sh")){
			return 1;
		}else{
			return 2;
		}
	}

}
