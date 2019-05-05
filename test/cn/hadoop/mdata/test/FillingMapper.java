package cn.hadoop.mdata.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;

import cn.hadoop.mdata.entity.DBDYData;

public class FillingMapper extends Configured implements Tool,MapRunnable<Text, DBDYData, Text, DBDYData>{

	@Override
	public void configure(JobConf arg0) {
		setConf(arg0);
		
	}

	@Override
	public void run(RecordReader<Text, DBDYData> arg0,
			OutputCollector<Text, DBDYData> arg1, Reporter arg2)
			throws IOException {
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
