package com.jike.telecom;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TelecomReducer extends Reducer<Text, Bean, Bean, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<Bean> value, Reducer<Text, Bean, Bean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Bean result = new Bean();
		for(Bean values:value){
			result.setReportTime(values.getReportTime());
			result.setAppType(values.getAppType());
			result.setAppSubtype(values.getAppSubtype());
			result.setCellid(values.getCellid());
			result.setUserIP(values.getUserIP());
			result.setUserPort(values.getUserPort());
			result.setAppServerIP(values.getAppServerIP());
			result.setAppServerPort(values.getAppServerPort());
			result.setHost(values.getHost());
			
			result.setAttempts(result.getAttempts()+values.getAttempts());
			result.setAccepts(result.getAccepts()+values.getAccepts());
			result.setTrafficUL(result.getTrafficUL()+values.getTrafficUL());
			result.setTrafficDL(result.getTrafficDL()+values.getTrafficDL());
			result.setRetranUL(result.getRetranUL()+values.getRetranUL());
			result.setRetranDL(result.getRetranDL()+values.getRetranDL());
			result.setTransDelay(result.getTransDelay()+values.getTransDelay());
		}
		context.write(result, NullWritable.get());
	}
}
