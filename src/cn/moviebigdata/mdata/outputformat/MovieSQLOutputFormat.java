package cn.moviebigdata.mdata.outputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import cn.hadoop.mdata.common.MysqlUtil;
import cn.hadoop.mdata.entity.DBDYData;
import cn.moviebigdata.mdata.writer.MysqlMovieRecoderWriter;

public class MovieSQLOutputFormat implements OutputFormat<Text, DBDYData>{

	private static final Log logger = LogFactory.getLog(MovieSQLOutputFormat.class);
	
	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
			throws IOException {
		
		
	}

	@Override
	public RecordWriter<Text, DBDYData> getRecordWriter(FileSystem fs,
			JobConf job, String task_nam, Progressable progress) throws IOException {
		Connection conn = null;
		try {
			conn = MysqlUtil.getInstance(job).init(job);
			if(conn!=null){
				logger.info("数据初始化成功......");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new MysqlMovieRecoderWriter(conn);
	}

}
