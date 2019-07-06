package com.jike.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Item, IntWritable, Item> {

	Map<String, Item> productMap = new HashMap<>();

	@Override
	protected void reduce(Text key, Iterable<Item> value, Reducer<Text, Item, IntWritable, Item>.Context context)
			throws IOException, InterruptedException {
		ArrayList<Item> list = new ArrayList<>();

		for (Item values : value) {
			Item items = values.clone();
			list.add(items);
			if (values.getOrderId().equals("")) {
				productMap.put(items.getGoodId(), items);
			}
		}
		for (Item values : list) {
			if (!values.getOrderId().equals("")) {
				Item item = new Item();
				item.setOrderId(values.getOrderId());
				item.setDate(values.getDate());
				item.setGoodId(values.getGoodId());
				item.setNum(values.getNum());
				item.setKind(productMap.get(values.getGoodId()).getKind());
				item.setPrice(productMap.get(values.getGoodId()).getPrice());
				
				System.out.println(item);
				context.write(new IntWritable(item.getDate()), item);

			}
		}
	}
}
