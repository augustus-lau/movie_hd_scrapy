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
		//�õ�һ�����ݿ������
		try {
			conn = MysqlUtil.getInstance(job).init(job);
			start = ((DBSplit)split).getStart();
			limit = ((DBSplit)split).getLimit();
			if(conn != null){
				logger.info("���ݿ��ʼ���ɹ�......");
			}else{
				throw new SQLException("���ݿ��ʼ��ʧ�ܣ�");
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
		
		//�����������������split����
		int split_min = sum%limit == 0 ? sum/limit : sum/limit+1;
		logger.info(".......................................ϵͳĬ�ϵ�splitsumΪ��\t" + numsplit);
		DBSplit[] splits = new DBSplit[split_min];
		for(int i = 0; i < split_min ;i++){
			DBSplit split = new DBSplit(start,limit);
			splits[i] = split;
			start = start+limit;
		}
		return splits;
	}

}
