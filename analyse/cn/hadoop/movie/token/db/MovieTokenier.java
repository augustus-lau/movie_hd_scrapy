package cn.hadoop.movie.token.db;


import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.hadoop.mdata.common.MysqlUtil;
import cn.hadoop.mdata.token.TokenUtil;
import cn.moviebigdata.mdata.conf.MovieConfiguration;
import cn.moviebigdata.mdata.inputformat.MysqlInputFormat;
import com.chenlb.mmseg4j.analysis.ComplexAnalyzer;
public class MovieTokenier extends Configured implements Tool{
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: movietokenizer <resultPath>");
			return -1;
		}
		try {
			tokenier(new Path(args[0]));
			
			return 0;
		} catch (Exception e) {
			System.err.println("MovieTokenier: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}

	private void tokenier(Path outputDir) throws IOException, ClassNotFoundException, SQLException{
		Configuration conf = getConf();
		JobConf tokenJob = new JobConf(conf);
		tokenJob.setJobName("tokenJob");
		
		FileOutputFormat.setOutputPath(tokenJob, outputDir);
		
		tokenJob.setMapperClass(TokenMapper.class);
		tokenJob.setReducerClass(TokenReducer.class);
		
		tokenJob.setMapOutputKeyClass(Text.class);
		tokenJob.setMapOutputValueClass(Text.class);
		tokenJob.setOutputKeyClass(Text.class);
		tokenJob.setOutputValueClass(Text.class);
		tokenJob.setInputFormat(MysqlInputFormat.class);
		tokenJob.setOutputFormat(TextOutputFormat.class);
		JobClient.runJob(tokenJob);
	    MysqlUtil.getInstance(conf).init(conf).close();
	}
	
	
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new MovieTokenier(),
				args);
		System.exit(res);
	}
	
	private static class TokenMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{
		
		private ComplexAnalyzer analyzer = new ComplexAnalyzer();
		
		@Override
		public void map(Text _key, Text content, OutputCollector<Text, Text> collect,
				Reporter report) throws IOException {
			String comment = content.toString();
			List<String> trems = TokenUtil.tokenToTerm(comment, analyzer);
			if(trems != null && trems.size()>0){
				for(String term : trems){
					collect.collect(new Text(term), new Text(_key));
				}
			}
		}
	}
	
	
	/**
	 * 统计词出现的总频率
	 * @author tk
	 *
	 */
	private static class TokenReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text term, Iterator<Text> keys,
				OutputCollector<Text, Text> collect, Reporter report)
				throws IOException {
			StringBuffer buffer = new StringBuffer();
			int sum = 0;
			while(keys.hasNext()){
				Text key = keys.next();
				buffer.append(key.toString()).append("::");
				sum ++;
			}
			collect.collect(new Text(term +"\t"+sum), new Text(buffer.toString()));
		}
		
	}

}
