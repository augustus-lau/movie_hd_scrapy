package cn.moviebigdata.mdata.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class ActorAvgScoreRecordWriter implements RecordWriter<FloatWritable, Text>{

	private static final Log logger = LogFactory.getLog(ActorAvgScoreRecordWriter.class);
	
	private static final String stssql = "insert into actoravg(avgscore,mcount,actor,scores,country) values(?,?,?,?,?)";
	
	private Connection conn =null;
	private PreparedStatement pstmt;
	private static int sum=0;
	
	public ActorAvgScoreRecordWriter(Connection conn) {
		if(conn!=null){
			this.conn = conn;
			try {
				pstmt = conn.prepareStatement(stssql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close(Reporter arg0) throws IOException {
		logger.info("数据库正在关闭......");
		if(pstmt!=null){
			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			};
		}
		if(conn!=null){
			try {
				conn.commit();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void write(FloatWritable _avg, Text _values) throws IOException {
		try {
			
			String[] values = _values.toString().split("\t");
			
			pstmt.setFloat(1, _avg.get());
			pstmt.setInt(2, Integer.parseInt(values[0]));
			pstmt.setString(3, values[1]);
			pstmt.setString(4, values[2]);
			pstmt.setString(5, values[3]);
			pstmt.execute();
			sum ++;
			if(sum >= 100){
				conn.commit();
				sum = 0;
			}
			
				
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}

}
