package cn.moviebigdata.mdata.writer;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import cn.hadoop.mdata.entity.DBDYReplyData;


/**
 * ���������Key���������ļ�������ļ������ڣ��򴴽�������ļ����ڣ����������
 * �������KeyΪ�ض��Ľ�����,��ر��ļ������
 * @author tk
 *
 */
public class MuiltKeySequenceRecordWriter implements org.apache.hadoop.mapred.RecordWriter<Text, DBDYReplyData>{
	
	private FileSystem fs;
	
	private JobConf job;
	
	private  Progressable progress;
	
	private Path basePath;
	
	private Path taskPath;
	
	private CompressionType cType;
	
	private Writer out;
	
	public MuiltKeySequenceRecordWriter(FileSystem fs, JobConf job,
			Progressable progress, Path basePath) {
		super();
		this.fs = fs;
		this.job = job;
		this.progress = progress;
		this.basePath = basePath;
		cType = SequenceFileOutputFormat.getOutputCompressionType(job);
	}

	@Override
	public void close(Reporter arg0) throws IOException {
		if(out!=null){
			out.close();
		}
		
	}

	@Override
	public void write(Text _key, DBDYReplyData _item) throws IOException {
		taskPath = new Path(basePath, _key.toString());
		if(taskPath == null){
			fs.create(taskPath);
		}
		out = SequenceFile.createWriter(fs, job, taskPath, Text.class, DBDYReplyData.class, cType, progress);
		out.append(_key, _item);
		
	}



}
