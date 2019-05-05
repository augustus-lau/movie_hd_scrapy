package cn.moviebigdata.mdata.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import cn.hadoop.mdata.entity.MData;
import cn.moviebigdata.mdata.reader.IMDBRecordReader;

/**
 * 获取IMDB的split 并将split传给RecordReader 读取数据
 * @author Administrator
 *
 */
public class IMDBInputFormat extends FileInputFormat<Text, MData> implements JobConfigurable{
	
	private CompressionCodecFactory compressionCodecs;
	
	@Override
	public void configure(JobConf jobconf) {
		compressionCodecs = new CompressionCodecFactory(jobconf);
	}

	@Override
	public RecordReader<Text, MData> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		reporter.setStatus(split.toString());
		return new IMDBRecordReader((FileSplit) split, job);
	}

	
	protected boolean isSplitable(FileSystem fs, Path file)
    {
        CompressionCodec codec = compressionCodecs.getCodec(file);
        if(null == codec)
            return true;
        else
            return codec instanceof SplittableCompressionCodec;
    }
	
}
