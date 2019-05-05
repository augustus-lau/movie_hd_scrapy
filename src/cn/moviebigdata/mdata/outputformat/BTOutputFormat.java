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
import org.apache.hadoop.util.Progressable;
import cn.hadoop.mdata.entity.BTData;

public class BTOutputFormat implements OutputFormat<Text, BTData> {

	private static final String BT_UN_REPLY_DIR = System.currentTimeMillis()+"";
	
	/**
	 * 检查输出目录是否存在，检查输出文件是否存在
	 */
	@Override
	public void checkOutputSpecs(FileSystem fs, JobConf jobconf)
			throws IOException {
		Path file = FileOutputFormat.getOutputPath(jobconf);
		if ((file == null) && (jobconf.getNumReduceTasks() != 0)) {
			throw new InvalidJobConfException(
					"BTOutputFormat OutPut Path directory not set in JobConf.");
		}

		if (fs == null) {
			fs = file.getFileSystem(jobconf);
		}

		if (fs.exists(new Path(file, BT_UN_REPLY_DIR)))
			throw new IOException("un_reply_data already fetched!");

	}

	@Override
	public RecordWriter<Text, BTData> getRecordWriter(FileSystem fs,
			JobConf job, String task_name, Progressable progressable)
			throws IOException {
		  
		  Path file = FileOutputFormat.getOutputPath(job);
		  final Path btoutdir = new Path(new Path(file, BT_UN_REPLY_DIR), task_name);//重新生成我们的输出路径
		  
		  /**
		   * 获取序列化文件默认的配置方式：mapred.output.compression.type中配置
		   * 共三种：NONE, RECORD or BLOCK.。默认为 RECORD
		   * 所以这里采用RECORD来压缩
		   */
//		  CompressionType cType = SequenceFileOutputFormat.getOutputCompressionType(job);
		  CompressionType cType = CompressionType.RECORD;
		  final Writer out = SequenceFile.createWriter(fs,job,btoutdir,Text.class,BTData.class,cType,progressable);
		  
		  
		return new RecordWriter<Text, BTData>(){

			@Override
			public void close(Reporter reporter) throws IOException {
				if(out!=null){
					out.close();
				}
			}

			@Override
			public void write(Text key, BTData value) throws IOException {
				out.append(key, value);
			}
			
		};
	}

}
