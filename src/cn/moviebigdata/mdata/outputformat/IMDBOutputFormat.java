package cn.moviebigdata.mdata.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

import cn.hadoop.mdata.entity.MData;

public class IMDBOutputFormat implements OutputFormat<Text,MData>{
	
	private static final String IMDB_UN_REPLY_DIR = System.currentTimeMillis()+"";
	
	@Override
	public void checkOutputSpecs(FileSystem fs, JobConf conf)
			throws IOException {
		Path path = FileOutputFormat.getOutputPath(conf);
		if(path == null){
			throw new InvalidJobConfException("IMDBOutputFormat Output Dir Not Found");
		}
		
		if(fs == null){
			fs = path.getFileSystem(conf);
		}
		
		if(fs.exists(new Path(path,IMDB_UN_REPLY_DIR))){
			throw new InvalidJobConfException("IMDBOutputFormat Output File exits!");
		}
		
	}

	@Override
	public RecordWriter<Text, MData> getRecordWriter(FileSystem fs,
			JobConf job, String task_name, Progressable progressable)
			throws IOException {
			
		Path file = FileOutputFormat.getOutputPath(job);
		final Path btoutdir = new Path(new Path(file, IMDB_UN_REPLY_DIR), task_name);//重新生成我们的输出路径
		CompressionType cType = SequenceFileOutputFormat.getOutputCompressionType(job);
		final Writer out = SequenceFile.createWriter(fs, job, btoutdir, Text.class, MData.class, cType, progressable);
		 
		return new RecordWriter<Text, MData>(){

			@Override
			public void close(Reporter reporter) throws IOException {
				if(out!=null){
					out.close();
				}
			}

			@Override
			public void write(Text key, MData _item) throws IOException {
				out.append(key, _item);
			}
		};
	}

}
