package cn.moviebigdata.mdata.reader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Stack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;

/**
 * 从数据库中读取数据的方法
 * 
 * 如果每次连接从mysql中读取一条数据，性能会很低效。但是如果一次性从数据库中全部取出limit条数据，内存又受不了
 * 
 * 所以我们采用的模式为，一次性读取20条数据，循环读取，直到limit条数据全部读取完成
 * 
 * @author LIU_SHUAI
 * 
 */
public class MysqlRecordReader implements RecordReader<Text, Text> {
	
	private static final Logger logger = Logger.getLogger(MysqlRecordReader.class);
	private static final int step = 50;
	private int start;
	private int length;
	private int pos;
	private String select_sql = "select mid, content from comments limit ";
	private Connection conn = null;

	private Stack<Comment> comments_cache;

	public MysqlRecordReader(int start, int length, Connection conn) {
		super();
		this.start = start;
		this.length = length;
		this.conn = conn;
		comments_cache = new Stack<Comment>();
	}

	@Override
	public synchronized void close() throws IOException {
//		if (conn != null) {
//			try {
//				conn.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
		
		logger.info("正在调用close方法......");
	}

	@Override
	public Text createKey() {

		return new Text();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public synchronized float getProgress() throws IOException {

		return (pos - start) / length;
	}

	@Override
	public boolean next(Text _key, Text _comment) throws IOException {

		/**
		 * 如果当前坐标小于总数据量，表明还有新数据
		 */
		if (pos < length) {
			/**
			 * 读取一行数据时，先判断缓存中时候还有数据，如果还有数据，则直接从缓存中取一条数据 否则从数据库加载数据到缓存
			 */
			if (comments_cache != null && comments_cache.size() > 0) {
				Comment _item = comments_cache.pop();
				_key.set(_item.mid);
				_comment.set(_item.content);
				logger.info("从缓存中读取数据......");
			} else {
				try {
					Comment _item = null;
					// 从数据库中批量获取下一批数据
					Statement stmt = conn.createStatement(); // 创建Statement对象
					ResultSet rs = stmt.executeQuery(select_sql + +start + ","
							+ step);
					while (rs.next()){
						_item = new Comment();
						_item.mid = rs.getString(1);
						_item.content = rs.getString(2);
						comments_cache.push(_item);
					}
					rs.close();
		            stmt.close();
		            logger.info("从数据库中读取数据......");
		            start = start + step; //重置开始位置
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				Comment  _target = comments_cache.pop();
				_key.set(_target.mid);
				_comment.set(_target.content);
				
			}
			pos++;//游标加1
			return true;
		}

		return false;
	}

	class Comment {
		String mid;
		String content;
	}

}
