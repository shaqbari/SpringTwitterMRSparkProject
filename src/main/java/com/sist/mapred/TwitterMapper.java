package com.sist.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sist.data.RankData;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one=new IntWritable(1);
	private Text result=new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		List<String> list = RankData.naverRank();
		Pattern[] p=new Pattern[list.size()];
		for (int a = 0; a < p.length; a++) {
			p[a]=Pattern.compile(list.get(a));
		}
		Matcher[] m=new Matcher[list.size()];
		for (int a = 0; a < m.length; a++) {
			m[a]=p[a].matcher(value.toString());
			while (m[a].find()) {
				result.set(m[a].group());//
				context.write(result, one);
			}
			
		}
		
		
	
	}

	
	
}
