package cn.hadoop.mdata.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import cn.hadoop.mdata.entity.DBDYReplyData;

public class MysqlUtil {
	
	private static final String DRIVER = "mysql.driver";
	private static final String URL = "mysql.url";
	private static final String USERNAME = "mysql.username";
	private static final String PASSWORD = "mysql.password";
	
	private static Connection conn =null;
	
	private static MysqlUtil instance=null;
	
	private static final String stssql = "insert into comment_00(fetch_status,fetch_level,fetch_msg,reply_id," +
			"mid,fromuser,reply_time,username,reply_valid,rating,content,userurl,device) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	
	private MysqlUtil(Configuration conf) {
		
	}

	public static synchronized MysqlUtil getInstance(Configuration conf){
		if(instance == null){
			instance = new MysqlUtil(conf);
			try {
				if(conn == null){
					conn = instance.init(conf);
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return instance;
	}

	public synchronized  Connection init(Configuration conf) throws ClassNotFoundException, SQLException{
		if(conn == null){
			Class.forName(conf.get(DRIVER,""));
			conn = DriverManager.getConnection(conf.get(URL), conf.get(USERNAME), conf.get(PASSWORD));
		}
		conn.setAutoCommit(false);
		return conn;
	}
	
	
	public synchronized void close() throws SQLException{
		if(conn!=null){
			conn.close();
		}
	}
	
	public synchronized  void write(List<DBDYReplyData> datas) throws SQLException{
		PreparedStatement pstmt = conn.prepareStatement(stssql);
		for(DBDYReplyData _item : datas){
			pstmt.setInt(1, _item.getFetch_status());
			pstmt.setInt(2, _item.getFetch_level());
			pstmt.setString(3, _item.getFetch_msg());
			pstmt.setString(4, _item.getReplyID());
			pstmt.setString(5, _item.getMid());
			pstmt.setString(6, _item.getFromuser());
			pstmt.setString(7, _item.getReply_time());
			pstmt.setString(8, _item.getUsername());
			pstmt.setString(9, _item.getReply_valid());
			pstmt.setString(10, _item.getRating());
			pstmt.setString(11, _item.getContent());
			pstmt.setString(12, _item.getUserurl());
			pstmt.setString(13, _item.getDevice());
			pstmt.execute();
		}
		conn.commit();
	}
	
	public Set<String> readfeched() throws SQLException{
		Set<String> fetcheds = new HashSet<String>();
		Statement stmt = conn.createStatement(); 
		ResultSet rs  = stmt.executeQuery("select mid from comment_00");
		while (rs.next()){
			if(!fetcheds.contains(rs.getString("mid"))){
				fetcheds.add(rs.getString("mid"));
			}
		}
		return fetcheds;
	}
	
}
