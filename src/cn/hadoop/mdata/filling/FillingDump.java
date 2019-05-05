package cn.hadoop.mdata.filling;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import cn.hadoop.mdata.entity.DBDYData;
import cn.moviebigdata.mdata.conf.MovieConfiguration;

public class FillingDump extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: FillingDump <sourcePath> <resultPath>");
			return -1;
		}
		try {
			dump(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("FillingDump: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new FillingDump(), args);
		System.exit(res);
	}
	
	
	private void dump(Path from,Path to) throws IOException{
		Configuration conf = this.getConf();
		JobConf dumpjob = new JobConf(conf);
		dumpjob.setJobName("BTFormatDump");
		
		FileInputFormat.addInputPath(dumpjob, from);
		FileOutputFormat.setOutputPath(dumpjob, to);
		
		dumpjob.setMapperClass(FillingDumpMapper.class);
		dumpjob.setReducerClass(FillingDumpReducer.class);
		dumpjob.setMapOutputKeyClass(Text.class);
		dumpjob.setMapOutputValueClass(DBDYData.class);
		dumpjob.setOutputKeyClass(Text.class);
		dumpjob.setOutputValueClass(SequenceFileOutputFormat.class);
		dumpjob.setInputFormat(SequenceFileInputFormat.class);
		JobClient.runJob(dumpjob);
	}
	
	public static class FillingDumpMapper extends MapReduceBase implements Mapper<Text, DBDYData, Text, DBDYData>{

		@Override
		public void map(Text _key, DBDYData _data,
				OutputCollector<Text, DBDYData> out, Reporter reporter)
				throws IOException {
			out.collect(_key, _data);
		}
		
	}
	
	public static class FillingDumpReducer extends MapReduceBase implements Reducer<Text, DBDYData, Text, Text>{

		@Override
		public void reduce(Text arg0, Iterator<DBDYData> _datas,
				OutputCollector<Text, Text> out, Reporter arg3)
				throws IOException {
			while(_datas.hasNext()){
				out.collect(arg0, new Text(_datas.next().toString()));
			}
		}

		
	}
}
