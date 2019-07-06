package com.jike.telecom;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TelecomMapper extends Mapper<LongWritable, Text, Text, Bean> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Bean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split("\\|");
		Bean bean = new Bean();
		
		FileSplit fs = (FileSplit) context.getInputSplit();
		String fileName = fs.getPath().getName();
		String reportTime = fileName.split("_")[1];

		
		//--封装日期属性
		bean.setReportTime(reportTime);
				//--小区id
		bean.setCellid(data[16]);
				//--应用大类
		bean.setAppType(Integer.parseInt(data[22]));
				//--应用子类
		bean.setAppSubtype(Integer.parseInt(data[23]));
				//--用户ip
		bean.setUserIP(data[26]);
				//--用户port
		bean.setUserPort(Integer.parseInt(data[28]));
				//--服务端ip
		bean.setAppServerIP(data[30]);
				//--服务端port
		bean.setAppServerPort(Integer.parseInt(data[32]));
				//--域名
		bean.setHost(data[58]);
				
				//--获取请求响应码，如果是103，表示是一次成功的HTTP请求
				int appTypeCode=Integer.parseInt(data[18]);
				//--传输状态码
				String transStatus=data[54];
				 
				 
				if(bean.getCellid()==null||bean.getCellid().equals("")){
					bean.setCellid("000000000");
				}
				//--设置尝试次数为1，最后在reduce中要统计一个用户总的尝试次数
				if(appTypeCode==103){
					bean.setAttempts(1);
				}
				
				//--设置接收次数为1，
				if(appTypeCode==103&&"10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306".contains(transStatus)){
					bean.setAccepts(1);
				}else{
					bean.setAccepts(0);
				}
				//--设置上行流量
				if(appTypeCode == 103){
					bean.setTrafficUL(Long.parseLong(data[33]));
				}
				//--设置下行流量
				if(appTypeCode == 103){
					bean.setTrafficDL(Long.parseLong(data[34]));
				}
				//--设置重传上行流量
				if(appTypeCode == 103){
					bean.setRetranUL(Long.parseLong(data[39]));
				}
				//--设置重传下行流量
				if(appTypeCode == 103){
					bean.setRetranDL(Long.parseLong(data[40]));
				}
				//--设置请求时长
				if(appTypeCode==103){
					bean.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
				}
				
				//--用标识key
				String userKey=bean.getReportTime() + "|" + bean.getAppType() + "|" + bean.getAppSubtype() + "|" + bean.getUserIP() + "|" + bean.getUserPort() + "|" + bean.getAppServerIP() + "|" + bean.getAppServerPort() +"|" + bean.getHost() + "|" + bean.getCellid();

				context.write(new Text(userKey),bean);
	}
}
