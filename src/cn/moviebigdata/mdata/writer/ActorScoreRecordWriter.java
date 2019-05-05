package cn.moviebigdata.mdata.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import cn.hadoop.mdata.entity.DBDYData;

public class ActorScoreRecordWriter implements RecordWriter<Text, DBDYData>{
	
	private static final Log logger = LogFactory.getLog(ActorScoreRecordWriter.class);
	
	private static final String stssql = "insert into actorscore(actor,mname,mscore,movieid) values(?,?,?,?)";
	
	private Connection conn =null;
	private PreparedStatement pstmt;
	private static int sum=0;
	
	public ActorScoreRecordWriter(Connection conn) {
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
	public void write(Text _actor, DBDYData _item) throws IOException {
		try {
			if(sum <100){
				pstmt.setString(1, _actor.toString());
				pstmt.setString(2, _item.getName());
				String score = _item.getRating();
				if(score == null || "".equals(score) || "-".equals(score)){
					pstmt.setFloat(3, 0);
				}else{
					pstmt.setFloat(3, Float.parseFloat(score));
				}
				pstmt.setString(4, _item.getMid());
				pstmt.execute();
				sum ++;
			}else{
				conn.commit();
				sum = 0;
			}
			
				
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

}
