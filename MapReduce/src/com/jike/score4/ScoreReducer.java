package com.jike.score4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ScoreReducer extends Reducer<Text, ScoreBean, Text, ScoreBean>{

	@Override
	protected void reduce(Text key, Iterable<ScoreBean> value, Reducer<Text, ScoreBean, Text, ScoreBean>.Context context)
			throws IOException, InterruptedException {

		ScoreBean result = new ScoreBean();
		result.setName(key.toString());
		for(ScoreBean values:value){
			result.setChinese(values.getChinese()+result.getChinese());
			result.setEnglish(values.getEnglish()+result.getEnglish());
			result.setMath(values.getMath()+result.getMath());
		}
		context.write(key,result);
//		Iterator<ScoreBean> it = value.iterator();
//		while(it.hasNext()){
//			ScoreBean next = it.next();
//			sb.setName(next.getName());
//			sb.setChinese(next.getChinese()+sb.getChinese());
//			sb.setEnglish(next.getEnglish()+sb.getEnglish());
//			sb.setChinese(next.getMath()+sb.getMath());
//		}
		
	}
}