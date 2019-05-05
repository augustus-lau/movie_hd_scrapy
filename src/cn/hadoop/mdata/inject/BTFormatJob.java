package cn.hadoop.mdata.inject;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.hadoop.mdata.entity.BTData;
import cn.moviebigdata.mdata.inputformat.BTMuiltLineInputFormat;
import cn.moviebigdata.mdata.outputformat.BTOutputFormat;

/**
 * 用于清洗BT天堂的数据
 * @author Administrator
 *
 */
public class BTFormatJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: btformat <sourcePath> <resultPath>");
			return -1;
		}
		try {
			washer(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("BTFormatJob: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BTFormatJob(), args);
		System.exit(res);
	}
	
	private void washer(Path from,Path to) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		System.out.println("BTFormatJob: starting at " + sdf.format(start));
		System.out.println("BTFormatJob: source: " + from);
		System.out.println("BTFormatJob: result: " + to);
		Configuration conf = this.getConf();
		JobConf washJob = new JobConf(conf);
		washJob.setJobName("BTFormatJob");
		FileInputFormat.addInputPath(washJob, from);
		FileOutputFormat.setOutputPath(washJob, to);
		
		washJob.setMapperClass(WasherMapper.class);
		washJob.setReducerClass(WasherReducer.class);
		
		washJob.setInputFormat(BTMuiltLineInputFormat.class);
		washJob.setOutputFormat(BTOutputFormat.class);
		
		washJob.setMapOutputKeyClass(Text.class);
		washJob.setMapOutputValueClass(BTData.class);
		
		washJob.setOutputKeyClass(Text.class);
		washJob.setOutputValueClass(BTData.class);
		
		JobClient.runJob(washJob);
	}
	
	public static class WasherMapper extends MapReduceBase implements Mapper<Text,BTData,Text,BTData>{

		@Override
		public void map(Text mid, BTData item,
				OutputCollector<Text, BTData> output, Reporter reporter)
				throws IOException {
			output.collect(mid, item);
		}
		
	}
	
	public static class WasherReducer extends MapReduceBase implements Reducer<Text,BTData,Text,BTData>{
		
		@Override
		public void reduce(Text mid, Iterator<BTData> items,
				OutputCollector<Text, BTData> output, Reporter reporter)
				throws IOException {
			BTData first = null;
			while(items.hasNext()){
				first = items.next();
				break;
			}
			if(first!=null){
				output.collect(mid, first);
			}else{
				return;
			}
		}
		
	}
}
