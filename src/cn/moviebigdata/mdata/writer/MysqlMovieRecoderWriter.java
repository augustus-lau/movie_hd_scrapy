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
import cn.hadoop.mdata.common.UtilTool;
import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.task.DBCommentTasker;

public class MysqlMovieRecoderWriter implements RecordWriter<Text, DBDYData>{
	
	private static final Log logger = LogFactory.getLog(DBCommentTasker.class);
	
	private static final String stssql = "insert into dbmovie(movie_id,fpage,mname,othername,image," +
			"actors,sorts,writers,directors,pub,country,descp,rate,imdbid,moviepath,discusspath,commentpath) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private Connection conn =null;
	private PreparedStatement pstmt;
	private static int sum=0;
	public MysqlMovieRecoderWriter(Connection conn) {
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
	public synchronized void close(Reporter arg0) throws IOException {
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
	public synchronized void write(Text _key, DBDYData _item) throws IOException {
		try {
			if(sum <100){
				pstmt.setString(1, _item.getMid());
				pstmt.setString(2, _item.getFrompage());
				pstmt.setString(3, _item.getName());
				pstmt.setString(4, _item.getOthername());
				pstmt.setString(5, _item.getImage());
				pstmt.setString(6, UtilTool.join("、",_item.getActors_text().toStrings()));
				pstmt.setString(7, UtilTool.join("、", _item.getSorts_text().toStrings()));
				pstmt.setString(8, UtilTool.join("、", _item.getWriters_text().toStrings()));
				pstmt.setString(9, _item.getDirector());
				String release = _item.getRelease();
				if(!"".equals(release) && !"-".equals(release) && release!=null){
					if(release.length()>10){
						release = release.substring(0, 10);
						pstmt.setString(10, release);
					}else{
						pstmt.setString(10, release);
					}
				}else{
					pstmt.setString(10, release);
				}
				
				
				pstmt.setString(11, _item.getCountry());
				
				String descTemp = _item.getDesc();
				if(!"".equals(descTemp) && !"-".equals(descTemp) && descTemp!=null){
					if(descTemp.length()>34 && descTemp.contains("<spanproperty")){
						descTemp = descTemp.substring(34, descTemp.length());
						pstmt.setString(12, descTemp);
					}else{
						pstmt.setString(12, descTemp);
					}
				}
				pstmt.setString(13, _item.getRating());
				pstmt.setString(14, _item.getImdbid());
				pstmt.setString(15, _item.getMovieurl());
				pstmt.setString(16, _item.getDiscussionurl());
				pstmt.setString(17, _item.getCommentsurl());
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
