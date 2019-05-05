package cn.hadoop.mdata.dump;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import cn.hadoop.mdata.entity.DBDYData;
import cn.moviebigdata.mdata.conf.MovieConfiguration;
import cn.moviebigdata.mdata.outputformat.MovieSQLOutputFormat;

public class CommentDump extends Configured implements Tool{

	private static final Log LOG = LogFactory.getLog(CommentDump.class);
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: BTFormatDump <sourcePath> <resultPath>");
			return -1;
		}
		try {
			dump(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("BTFormatDump: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}
	
	
	private void dump(Path from,Path to) throws IOException{
		Configuration conf = this.getConf();
		JobConf dumpjob = new JobConf(conf);
		dumpjob.setJobName("BTFormatDump");
		
		FileInputFormat.addInputPath(dumpjob, from);
//		FileOutputFormat.setOutputPath(dumpjob, to);
		
		dumpjob.setMapperClass(DumpMapper.class);
		dumpjob.setCombinerClass(DumpReducer.class);
		dumpjob.setReducerClass(DumpReducer.class);
		
		dumpjob.setMapOutputKeyClass(Text.class);
		dumpjob.setMapOutputValueClass(DBDYData.class);
		
		dumpjob.setOutputKeyClass(Text.class);
		dumpjob.setOutputValueClass(DBDYData.class);
		
		dumpjob.setInputFormat(SequenceFileInputFormat.class);
		dumpjob.setOutputFormat(MovieSQLOutputFormat.class);
		JobClient.runJob(dumpjob);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new CommentDump(), args);
		System.exit(res);
	}
	
	
	private static class DumpMapper extends MapReduceBase implements Mapper<Text, DBDYData, Text, DBDYData>{

		@Override
		public void map(Text _key, DBDYData _data,
				OutputCollector<Text, DBDYData> out, Reporter reporter)
				throws IOException {
//			Text cid = new Text(_data.getReplyID());
			out.collect(_key, _data);
		}
		
	}
	
	private static class DumpReducer extends MapReduceBase implements Reducer<Text, DBDYData, Text, DBDYData>{

		@Override
		public void reduce(Text arg0, Iterator<DBDYData> _datas,
				OutputCollector<Text, DBDYData> out, Reporter arg3)
				throws IOException {
			while(_datas.hasNext()){
				out.collect(arg0, _datas.next());
				return;
			}
			
		}

		
	}
}
