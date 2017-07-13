package com.sist.hive;

import java.util.*;

import org.springframework.stereotype.Component;

import java.io.*;
import java.sql.*;

@Component
public class TwitterDAO {
	private String driver="org.apache.hive.jdbc.HiveDriver";
	private String url="jdbc:hive2://localhost:10000/default";
	
	private Connection conn;
	private Statement stmt;
	
	//Driver 연동
	public TwitterDAO(){
		try {
			Class.forName(driver);
			
			
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void getConnection(){
		try {
			conn=DriverManager.getConnection(url, "hive", "hive");
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
	}
	
	public void disConnection(){
		try {
			if (stmt!=null) {
				stmt.close();
			}
			if (conn!=null) {
				conn.close();
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void twitterCreateTable(String tName){
		try {
			getConnection();
			//먼저 테이블을 날린다.
			/*String sql="DROP TABLE "+tName;
			stmt=conn.createStatement();
			stmt.executeQuery(sql);
			stmt.close();*/
			//테이블이 있을때만 날린다.
			
			
			String sql="Create Table "+tName+"(data string, count int)"
					+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";//,로구분해서 집어넣겠다.
			stmt=conn.createStatement();
			stmt.executeQuery(sql);//hive에서는 파일을 읽어서 집어넣어야 하기 때문에  무조건 executeQuery이다. executeUpdate는 없다.
			//stmt.close();
			
			/*sql="LOAD DATA LOCAL INPATH './input/"+tName+".csv'" // ./을 인식못할수도 있다.
				+ " OVERWRITE INTO TABLE "+tName;
			stmt=conn.createStatement();
			stmt.executeQuery(sql);
			*/
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			disConnection();
		}	
	}
	
	public void twitterDataInsert(String tName){
		try {
			getConnection();

			String sql="LOAD DATA LOCAL INPATH '/home/sist/bigdataDev/SpringTwitterMRSparkProject/input/"+tName+".csv'"
				+ " OVERWRITE INTO TABLE "+tName;
			stmt=conn.createStatement();
			stmt.executeQuery(sql);
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			disConnection();
		}	
		
	}
	
	public List<TwitterVO> twitterRankData(){
		List<TwitterVO> list=new ArrayList<TwitterVO>();

		try {
			getConnection();
			String sql="Select d.data, d.count+n.count"
					+ " FROM daum d, naver n"
					+ " Where d.data=n.data";
			stmt=conn.createStatement();
			ResultSet rs=stmt.executeQuery(sql);
			while (rs.next()) {
				TwitterVO vo=new TwitterVO();
				vo.setRankdata(rs.getString(1));
				vo.setCount(rs.getInt(2));
				list.add(vo);
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			disConnection();
		}
		
		return list;
	}
	
}

















