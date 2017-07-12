package com.sist.twitter;



import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.stereotype.Component;

import com.sist.spark.TwitterSpark;

@Component
public class MainClass {
	@Autowired
	private TwitterSpark ts;
	
	@Autowired
	private Configuration conf;
	
	@Autowired
	private JobRunner jr;
	
	public static void main(String[] args) {
		String[] xml={"app.xml", "app-hadoop.xml"};
		ApplicationContext app=new ClassPathXmlApplicationContext(xml);
		
		MainClass m=(MainClass)app.getBean("mainClass");
		m.hadoopFileDelete();
		m.hadoopCopyFromLocal();
		
		try {
			m.jr.call();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
		m.hadoopCopyToLocal();
		m.createNameCSV();
		
	}
	
	public void hadoopFileDelete(){
		try {
			//하둡에서 지우기
			FileSystem fs=FileSystem.get(conf);
			if (fs.exists(new Path("/twitter_input_ns1"))) {
				fs.delete(new Path("/twitter_input_ns1"), true);//이미 있다면 폴더째로지운다. -rmr :  rm -rf
				
			}
			if (fs.exists(new Path("/twitter_output_ns1"))) {
				fs.delete(new Path("/twitter_output_ns1"), true);//이미 있다면 폴더째로지운다. -rmr :  rm -rf
				
			}
			fs.close();
			
			//spark결과 파일 지우기
			File dir=new File("./output_daum");
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
			e.printStackTrace();
		}
		
	};

	public void hadoopCopyFromLocal() {
		try {
			FileSystem fs=FileSystem.get(conf);
			fs.copyFromLocalFile(new Path("./input/naver.txt"), new Path("/twitter_input_ns1/naver.txt"));

			fs.close();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void hadoopCopyToLocal() {
		try {
			FileSystem fs=FileSystem.get(conf);
			fs.copyToLocalFile(new Path("/twitter_output_ns1/part-r-00000"), new Path("./input/naver"));
			
			fs.close();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void createNameCSV(){
		try {
			System.out.println("들어오나?");
			FileReader fr=new FileReader("./input/naver");
			int i=0;
			String data="";
			while ((i=fr.read())!=-1) {
				data+=String.valueOf((char)i);
				System.out.println(data);
			}
			fr.close();
			/*data=data.replace(" ", ",");*/
			System.out.println(data);
			
			
			String[] str=data.split("\n");
			//공백이 있으면 csv파일 만들기 어려우므로 mapper에서 단어를""로 묶고 나오자
			String sss="";
			for (String s : str) {
				StringTokenizer st=new StringTokenizer(s);//공백을 잘라준다.
				sss+=st.nextToken().trim()+","+st.nextToken().trim()+"\n";
			}
			sss=sss.replace("\"", "");//다시 따옴표 지운다.
			System.out.println(sss);
		
		
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
}





