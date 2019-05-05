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

import cn.hadoop.mdata.entity.BTData;
import cn.moviebigdata.mdata.reader.BTMuiltLineRecordReader;

public class BTMuiltLineInputFormat extends FileInputFormat<Text, BTData> implements JobConfigurable{
	
	
	public BTMuiltLineInputFormat() {
		compressionCodecs = null;
	}

	@Override
	public void configure(JobConf jobconf) {
		 compressionCodecs = new CompressionCodecFactory(jobconf);
	}

	@Override
	public RecordReader<Text, BTData> getRecordReader(InputSplit inputsplit,
			JobConf jobconf, Reporter reporter) throws IOException {
		reporter.setStatus(inputsplit.toString());
		return new BTMuiltLineRecordReader(jobconf,(FileSplit) inputsplit);
	}

	protected boolean isSplitable(FileSystem fs, Path file)
    {
        CompressionCodec codec = compressionCodecs.getCodec(file);
        if(null == codec)
            return true;
        else
            return codec instanceof SplittableCompressionCodec;
    }
	
	private CompressionCodecFactory compressionCodecs;
}
