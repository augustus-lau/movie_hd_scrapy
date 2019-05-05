package cn.hadoop.mdata.filling;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.queue.DBFillingFeeder;
import cn.hadoop.mdata.queue.Queue;
import cn.hadoop.mdata.task.DBFillingTasker;
import cn.moviebigdata.mdata.conf.MovieConfiguration;

public class DBDYFilling extends Configured implements Tool,MapRunnable<Text, DBDYData, Text, DBDYData>{
	
	private static final Log logger = LogFactory.getLog(DBDYFilling.class);
	
	private DBFillingFeeder feeder;
	private Queue<DBDYData> queue;
	
	AtomicInteger alivesum = new AtomicInteger(0);
	AtomicInteger spin_wait = new AtomicInteger(0); //默认为零
	AtomicInteger halt_sum = new AtomicInteger(0); //默认为零
	AtomicInteger fetch_total = new AtomicInteger(0); //默认为零
	
	@Override
	public void configure(JobConf arg0) {
		setConf(arg0);
		
	}


	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length < 2) {
			System.err.println("Usage: DBDYFilling <sourcePath> <resultPath>");
			return -1;
		}
		try {
			filling(new Path(arg0[0]), new Path(arg0[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("DBDYFilling: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}

	private void filling(Path from , Path to) throws IOException{
		Configuration conf = getConf();
		JobConf job = new JobConf(conf);
		FileInputFormat.addInputPath(job, from);
		FileOutputFormat.setOutputPath(job, to);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setMapRunnerClass(DBDYFilling.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DBDYData.class);		
		JobClient.runJob(job);
		
	}
	

	@Override
	public void run(RecordReader<Text, DBDYData> reader,
			OutputCollector<Text, DBDYData> collect, Reporter report)
			throws IOException {
		
		queue = new Queue<DBDYData>(getConf());
		feeder = new DBFillingFeeder(queue,reader,getConf());
		Thread feedThread = new Thread(feeder);
		feedThread.setDaemon(true);
		feedThread.setName("QueueFeeder");
		feedThread.start();
		int thread_num = Integer.parseInt(getConf().get("queen.thread.max",
				"10"));// 获取最大线程数
		logger.debug("init queue thread max sum :"
				+ getConf().get("queen.thread.max", "10"));
		
		for (int i = 0; i < thread_num; i++) {
			Thread tasker = new Thread(new DBFillingTasker(queue, feeder, collect,
					alivesum, spin_wait,halt_sum,fetch_total));
			tasker.start();
		}
		
		while (halt_sum.get() < thread_num) {
			try {
				logger.info("heart breaking :\t -fetch_total= "+fetch_total.get()+"\t -queue-size = "
						+ queue.getQueueSize() + "\t" + "-alive-thread = "
						+ alivesum.get() + "\t" + "-spin-wait = "
						+ spin_wait.get() + "\t" + "-halt-thread = " + halt_sum.get());
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
		}
		;
	}
	
	public static void main(String[] args) throws Exception {
		int recode = ToolRunner.run(MovieConfiguration.create(), new DBDYFilling(), args);
		System.exit(recode);
	}
	
}
