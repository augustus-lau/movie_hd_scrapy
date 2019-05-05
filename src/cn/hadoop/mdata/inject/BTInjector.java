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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.hadoop.mdata.entity.BTItem;
import cn.hadoop.mdata.queue.Queue;
import cn.hadoop.mdata.queue.QueueFeeder;
import cn.moviebigdata.mdata.conf.MovieConfiguration;
import cn.moviebigdata.mdata.request.BTTasker;

/**
 * 像这种起线程来执行Mapper的任务，可以没有Reduce程序，直接将数据输出到文件中
 * job.setMapRunnerClass(BTInjector);
 * 
 * @author liu_shuai
 * 
 */
public class BTInjector extends Configured implements Tool,
		MapRunnable<LongWritable, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(BTInjector.class);

	AtomicInteger alivesum = new AtomicInteger(0);
	AtomicInteger spin_wait = new AtomicInteger(0); //默认为零
	AtomicInteger halt_sum = new AtomicInteger(0); //默认为零
	private Queue<BTItem> queue;
	private QueueFeeder feeder;

	@Override
	public void run(RecordReader<LongWritable, Text> reader,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		queue = new Queue<BTItem>(getConf());
		feeder = new QueueFeeder(queue, reader, getConf());
		Thread feedThread = new Thread(feeder);
		feedThread.setDaemon(true);
		feedThread.setName("QueueFeeder");
		feedThread.start();
		int thread_num = Integer.parseInt(getConf().get("queen.thread.max",
				"10"));// 获取最大线程数
		LOG.debug("init queue thread max sum :"
				+ getConf().get("queen.thread.max", "10"));

		for (int i = 0; i < thread_num; i++) {
			Thread tasker = new Thread(new BTTasker(queue, feeder, output,
					alivesum, spin_wait,halt_sum));
			tasker.start();
		}

		// 监听线程池的变化,因为Task线程都是后台线程，所以必须确保所有的线程都空闲的时候才能停止
		while (halt_sum.get() < thread_num) {
			try {
				LOG.info("heart breaking : \t -queue-size = "
						+ queue.getQueueSize() + "\t" + "-alive-thread = "
						+ alivesum.get() + "\t" + "-spin-wait = "
						+ spin_wait.get() + "\t" + "-halt-thread = " + halt_sum.get());
				Thread.sleep(8000);
			} catch (InterruptedException e) {
			}
		}
		;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: btinjector <sourcePath> <resultPath>");
			return -1;
		}
		try {
			inject(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("BTInjector: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}

	private void inject(Path source, Path result) throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		System.out.println("BTInjector: starting at " + sdf.format(start));
		System.out.println("BTInjector: source: " + source);
		System.out.println("BTInjector: result: " + result);
		JobConf job = new JobConf(getConf());
		job.setJobName("BTInjector");
		FileInputFormat.addInputPath(job, source);
		FileOutputFormat.setOutputPath(job, result);
		job.setInputFormat(TextInputFormat.class);
		job.setMapRunnerClass(BTInjector.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		JobClient.runJob(job);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new BTInjector(),
				args);
		System.exit(res);
	}

	@Override
	public void configure(JobConf jobconf) {
		setConf(jobconf);
	}

}
