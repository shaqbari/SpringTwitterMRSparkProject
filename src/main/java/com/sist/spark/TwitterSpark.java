package com.sist.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.sist.data.RankData;

import au.com.bytecode.opencsv.CSVWriter;
import scala.Tuple2;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//daum랭크
@Component
@Scope("prototype")//실제프로그램은 여러명이 요청할수 있으므로 prototype으로 생성해야 한다. 싱글톤이면 다른사람끝날때까지 기다려야 한다.
public class TwitterSpark implements Serializable{//Spring에서 실행하려면 Serializable을 상속받아야 한다.
	public static void main(String[] args) {
		TwitterSpark ts=new TwitterSpark();
		
		try {
			File dir=new File("./output_daum_ns1");
			if (dir.exists()) {
				File[] list=dir.listFiles();
				//리눅스는 폴더 안의 내용을 모두 다 지우고 rm -rf
				for (File f : list) {
					f.delete();
				}
				dir.delete();//폴더를 지워야 한다. 
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
		ts.execute();
		ts.createCSV();
	}
	
	
	public void execute(){
		SparkConf conf=new SparkConf().setAppName("daum").setMaster("local");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> files=sc.textFile("./input/daum.txt");//다시 실행하려면 파일을 지워야한다 aop로 처리
		
		List<String> list=RankData.daumRank();
		final Pattern[] p=new Pattern[list.size()];//내부익명클래스에서 접근하기 위해 final을 준다.
		for (int a = 0; a < p.length; a++) {
			p[a]=Pattern.compile(list.get(a));
						
		}
		
		final List<String> wordList=new ArrayList<String>();//내부 클래스에서 쓸거라서 final을 써야 한다.
		JavaRDD<String> words=files.flatMap(new FlatMapFunction<String, String>() {
			//List<String> wordList=new ArrayList<String>();

			//아래매소드가 들어온 줄수만큼 생성된다.
			@Override
			public Iterable<String> call(String s) throws Exception {
			//((211).(238).(142).(98))
				System.out.println(s);
				Matcher[] m=new Matcher[p.length];
				for (int a = 0; a < m.length; a++) {
					m[a]=p[a].matcher(s);//s는 한줄
					while (m[a].find()) {//한줄에 여러개일 경우를 위해서 while문을 돌린다.
						wordList.add(m[a].group());
						System.out.println(" ; find : "+m[a].group());
					}
					
				}
					
				return wordList;
			}
		});
		
		System.out.println("wordList크기는 : "+wordList.size());
		
		//자른거에 1씩 부여
		JavaPairRDD<String, Integer> counts=words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		//=====================================Mapper
		
		//자료가 많을경우 Long형이 올수도 있다.
		JavaPairRDD<String, Integer> res=counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer sum, Integer i) throws Exception {
				return sum+i;
			}
		});
		
		//res.join(res);// selfjoin안된다. res대신 naver걸 join할 수 있다.
		
		res.saveAsTextFile("./output_daum_ns1/");//하둡에 저장?
		
		
		
	}
	
	public void createCSV() {
		try {
			/*StringWriter s=new StringWriter();
			CSVWriter cw=new CSVWriter(s);//까다로우니 나중에*/
			String data="";
			FileReader fr=new FileReader("./output_daum_ns1/part-00000");
			int i=0;
			while ((i=fr.read())!=-1) {
				data+=String.valueOf((char)i);//ascii -> char
			}
			fr.close();
			data=data.replace("(", "");
			data=data.replace(")", "");
			System.out.println(data);
			FileWriter fw=new FileWriter("./input/daum.csv");
			fw.write(data);
			fw.close();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}



