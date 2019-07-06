package com.jike.Totalsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TotalsortPartitioner extends Partitioner<IntWritable, IntWritable>{

	@Override
	public int getPartition(IntWritable key, IntWritable value, int num) {
		// TODO Auto-generated method stub
		if(key.get()<100){
			return 0;
		}
		if(key.get()<1000){
			return 1;
		}else{
			return 2;
		}
	}

}
