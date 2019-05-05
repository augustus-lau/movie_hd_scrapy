package cn.hadoop.movie.analyse.sina;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import cn.hadoop.mdata.entity.SinaItem;
import cn.moviebigdata.mdata.conf.MovieConfiguration;


/**
 * 如果进不去Mapper,是因为在F盘根目录下生成了一个临时tmp文件
 * 这个文件保存了以前hadoop未完成任务的信息，需要把这个文件全部删除掉
 * @author Administrator
 *
 */
public class GenreAnalyse extends Configured implements Tool{

	private static final Log logger = LogFactory.getLog(GenreAnalyse.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("Usage : genraAnalyse <-input> <-output>");
			return -1;
		}
		try {
			analyse(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("GenreAnalyse: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new GenreAnalyse(),
				args);
		System.exit(res);
	}
	
	private void analyse(Path from ,Path to) throws IOException{
		Configuration conf = getConf();
		JobConf job = new JobConf(conf);
		job.setJobName("genraAnalyse");
		FileInputFormat.addInputPath(job, from);
		FileOutputFormat.setOutputPath(job, to);
		
		job.setMapperClass(AnalyseMapper.class);
		job.setReducerClass(AnalyseReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SinaItem.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		JobClient.runJob(job);
	}
	
	
	private static class AnalyseMapper extends MapReduceBase implements Mapper<Text, SinaItem, Text, SinaItem>{

		@Override
		public void map(Text _key, SinaItem _item,
				OutputCollector<Text, SinaItem> collect, Reporter reporter)
				throws IOException {
			String genres = _item.getGenre();
			if(!org.apache.commons.lang.StringUtils.isEmpty(genres)){
				String [] arr = genres.trim().replaceAll("\r\t\n", "").replaceAll(" +","").split("/");
				for(String genre : arr){
//					logger.info("mapper\tgenre:=" + genre  + "\t _key:=" + _key);
					collect.collect(new Text(genre), _item);
				}
			}
		}
		
	}
	
	
	private static class AnalyseReducer extends MapReduceBase implements Reducer<Text, SinaItem, Text, Text>{

		/**
		 * 这里必须调用next()方法 ，如果不调用next()方法,那么item.hasNext()将进入死循环中，因为游标永远不会移动到下一个
		 */
		@Override
		public void reduce(Text _genre, Iterator<SinaItem> _items,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			logger.info("reducer comming....");
			long total_count = 0;
			double total_score = 0;
			while (_items.hasNext()) {
				SinaItem _item = _items.next();
				total_count++;
				String dscore = _item.getDouban_score();
				String iscore = _item.getImdb_score();
				double dds = 0;
				double dis = 0;
				if(org.apache.commons.lang.StringUtils.isNotBlank(iscore)){
					dis = Double.parseDouble(iscore);
				}
				if(org.apache.commons.lang.StringUtils.isNotBlank(dscore)){
					dds = Double.parseDouble(dscore);
				}
				
				total_score += (dis + dds);
				logger.info("reducer dbscore...." + _item.getDouban_score());
			}
			output.collect(_genre, new Text(total_count + "\t"+total_score+""));
		}

		
	}
}
