package cn.hadoop.mdata.inject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.queue.DBQueenFeeder;
import cn.hadoop.mdata.queue.Queue;
import cn.hadoop.mdata.task.DBInjectTasker;
import cn.moviebigdata.mdata.conf.MovieConfiguration;

/**
 * 关于豆瓣电影数据、影评的抓取
 * 根据文件读取豆瓣电影的起始年限与结束年限
 * @author Administrator
 *
 */
public class DBDYInjector extends Configured implements Tool,MapRunnable<LongWritable, Text, Text, DBDYData> {
	
	private static final Log LOG = LogFactory.getLog(DBDYInjector.class);
	
	private DBQueenFeeder feeder;
	private Queue<String> queue;
	
	AtomicInteger alivesum = new AtomicInteger(0);
	AtomicInteger spin_wait = new AtomicInteger(0); //默认为零
	AtomicInteger halt_sum = new AtomicInteger(0); //默认为零
	AtomicInteger fetch_total = new AtomicInteger(0); //默认为零
	
	@Override
	public void configure(JobConf jobconf) {
		setConf(jobconf);
		
	}

	@Override
	public void run(RecordReader<LongWritable, Text> reader,
			OutputCollector<Text, DBDYData> collect, Reporter reporter) throws IOException {
		
		queue = new Queue<String>(getConf());
		feeder = new DBQueenFeeder(queue, reader, getConf());
		Thread feedThread = new Thread(feeder);
		feedThread.setDaemon(true);
		feedThread.setName("QueueFeeder");
		feedThread.start();
		int thread_num = Integer.parseInt(getConf().get("queen.thread.max",
				"10"));// 获取最大线程数
		LOG.debug("init queue thread max sum :"
				+ getConf().get("queen.thread.max", "10"));
		//与此启动线程每个线程针对一个年份去抓取，直到将改年份数据抓取完为止，然后获取另一年份数据开始抓取
		for (int i = 0; i < thread_num; i++) {
			Thread tasker = new Thread(new DBInjectTasker(queue, feeder, collect,
					alivesum, spin_wait,halt_sum,fetch_total));
			tasker.start();
		}
		// 监听线程池的变化,因为Task线程都是后台线程，所以必须确保所有的线程都空闲的时候才能停止
		while (halt_sum.get() < thread_num) {
			try {
				LOG.info("heart breaking :\t -fetch_total= "+fetch_total.get()+"\t -queue-size = "
						+ queue.getQueueSize() + "\t" + "-alive-thread = "
						+ alivesum.get() + "\t" + "-spin-wait = "
						+ spin_wait.get() + "\t" + "-halt-thread = " + halt_sum.get());
				Thread.sleep(8000);
			} catch (InterruptedException e) {
			}
		}
		;
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
		job.setMapRunnerClass(DBDYInjector.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DBDYData.class);
		JobClient.runJob(job);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: dbinjector <sourcePath> <resultPath>");
			return -1;
		}
		try {
			inject(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("DBDYInjector: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new DBDYInjector(),
				args);
		System.exit(res);
	}
}
