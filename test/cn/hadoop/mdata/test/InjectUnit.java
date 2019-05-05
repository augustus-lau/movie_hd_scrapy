package cn.hadoop.mdata.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import cn.hadoop.mdata.entity.DBDYData;
import cn.moviebigdata.mdata.conf.MovieConfiguration;
import cn.moviebigdata.mdata.parse.DBDYParseone;
import cn.moviebigdata.mdata.request.DBListRequest;
import cn.moviebigdata.mdata.request.Request;
import cn.moviebigdata.mdata.request.RequestFactory;



public class InjectUnit extends Configured implements Tool,MapRunnable<LongWritable, Text, Text, DBDYData>{
	
	private Request request;
	
	@Override
	public void configure(JobConf arg0) {
		setConf(arg0);
		
	}

	@Override
	public void run(RecordReader<LongWritable, Text> arg0,
			OutputCollector<Text, DBDYData> collect, Reporter arg2)
			throws IOException {
		request = RequestFactory.createRequest(
				DBListRequest.class, "http://www.douban.com/tag/2012/movie");
		String result = request.request();
		try {
			List<DBDYData> items = DBDYParseone.getResult(result);
			for(DBDYData _item : items){
				collect.collect(new Text(_item.getMid()), _item);
			}
		} catch (Exception e) {
		}
		
	}
	
	/**
	 * F:/nutch2.3/douban/seed.txt F:/nutch2.3/douban/doubandb
	 */
	@Override
	public int run(String[] args) throws Exception {
		inject(new Path("F:/nutch2.3/douban/seed.txt"), new Path("F:/nutch2.3/douban/junittestdb"));
		return 0;
	}

	
	private void inject(Path from,Path to) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		System.out.println("BTInjector: starting at " + sdf.format(start));
		System.out.println("BTInjector: source: " + from);
		System.out.println("BTInjector: result: " + to);
		JobConf job = new JobConf(getConf());
		job.setJobName("BTInjector");
		FileInputFormat.addInputPath(job, from);
		FileOutputFormat.setOutputPath(job, to);
		job.setInputFormat(TextInputFormat.class);
		job.setMapRunnerClass(InjectUnit.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DBDYData.class);
		JobClient.runJob(job);
	}
	
	@Test
	public void test() throws Exception{
		int res = ToolRunner.run(MovieConfiguration.create(), new InjectUnit(),
				new String[]{});
		System.exit(res);
	}
}
