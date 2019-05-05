package cn.moviebigdata.mdata.inputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import cn.hadoop.mdata.common.MysqlUtil;
import cn.moviebigdata.mdata.reader.MysqlRecordReader;

public class MysqlInputFormat implements InputFormat<Text, Text>{

	private Logger logger = Logger.getLogger(MysqlInputFormat.class);
	private static final String MYSQL_SPLIT_SIZE = "mysql.read.split.size";
	private static final String MYSQL_DATA_SUM = "mysql.data.sum";
	
	
	
	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter report) throws IOException {
		Connection conn = null;
		int start = 0;
		int limit = 0;
		//得到一个数据库的链接
		try {
			conn = MysqlUtil.getInstance(job).init(job);
			start = ((DBSplit)split).getStart();
			limit = ((DBSplit)split).getLimit();
			if(conn != null){
				logger.info("数据库初始化成功......");
			}else{
				throw new SQLException("数据库初始化失败！");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new MysqlRecordReader(start,limit,conn);
	}
	
	
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numsplit) throws IOException {
		
		int start = 0;
		int limit = Integer.parseInt(job.get(MYSQL_SPLIT_SIZE));
		int sum = Integer.parseInt(job.get(MYSQL_DATA_SUM));
		
		//根据数据量计算出的split个数
		int split_min = sum%limit == 0 ? sum/limit : sum/limit+1;
		logger.info(".......................................系统默认的splitsum为：\t" + numsplit);
		DBSplit[] splits = new DBSplit[split_min];
		for(int i = 0; i < split_min ;i++){
			DBSplit split = new DBSplit(start,limit);
			splits[i] = split;
			start = start+limit;
		}
		return splits;
	}

}
