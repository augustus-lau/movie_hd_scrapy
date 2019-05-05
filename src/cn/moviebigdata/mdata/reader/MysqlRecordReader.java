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
 * �����ݿ��ж�ȡ���ݵķ���
 * 
 * ���ÿ�����Ӵ�mysql�ж�ȡһ�����ݣ����ܻ�ܵ�Ч���������һ���Դ����ݿ���ȫ��ȡ��limit�����ݣ��ڴ����ܲ���
 * 
 * �������ǲ��õ�ģʽΪ��һ���Զ�ȡ20�����ݣ�ѭ����ȡ��ֱ��limit������ȫ����ȡ���
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
		
		logger.info("���ڵ���close����......");
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
		 * �����ǰ����С��������������������������
		 */
		if (pos < length) {
			/**
			 * ��ȡһ������ʱ�����жϻ�����ʱ�������ݣ�����������ݣ���ֱ�Ӵӻ�����ȡһ������ ��������ݿ�������ݵ�����
			 */
			if (comments_cache != null && comments_cache.size() > 0) {
				Comment _item = comments_cache.pop();
				_key.set(_item.mid);
				_comment.set(_item.content);
				logger.info("�ӻ����ж�ȡ����......");
			} else {
				try {
					Comment _item = null;
					// �����ݿ���������ȡ��һ������
					Statement stmt = conn.createStatement(); // ����Statement����
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
		            logger.info("�����ݿ��ж�ȡ����......");
		            start = start + step; //���ÿ�ʼλ��
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				Comment  _target = comments_cache.pop();
				_key.set(_target.mid);
				_comment.set(_target.content);
				
			}
			pos++;//�α��1
			return true;
		}

		return false;
	}

	class Comment {
		String mid;
		String content;
	}

}
