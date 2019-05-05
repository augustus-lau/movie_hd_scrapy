package cn.moviebigdata.mdata.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import cn.hadoop.mdata.entity.DBDYReplyData;
import cn.moviebigdata.mdata.writer.MuiltKeySequenceRecordWriter;

public class MuiltKeySequenceOutputFormat implements OutputFormat<Text,DBDYReplyData>{
	
	private static final String BASE_PREFIX = "comments_muilt";
	
	@Override
	public void checkOutputSpecs(FileSystem fs, JobConf job)
			throws IOException {
		
		//判断文件是否存在
		Path outpath = FileOutputFormat.getOutputPath(job);
		if(outpath == null){
			throw new InvalidJobConfException("MuiltKeySequenceOutputFormat Output Dir Not Found");
		}
		
		if(fs == null){
			fs = outpath.getFileSystem(job);
		}
		
		//检查根目录是否存在
		if(fs.exists(new Path(outpath,BASE_PREFIX))){
			throw new InvalidJobConfException("MuiltKeySequenceOutputFormat Output File exits!");
		}
	}

	@Override
	public RecordWriter<Text, DBDYReplyData> getRecordWriter(FileSystem fs,
			JobConf job, String task_name, Progressable progress) throws IOException {
		Path basepath = FileOutputFormat.getOutputPath(job);
		return new MuiltKeySequenceRecordWriter(fs,job,progress,basepath);
	}

}
