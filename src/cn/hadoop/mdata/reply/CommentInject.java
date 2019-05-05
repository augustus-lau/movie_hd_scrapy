package cn.hadoop.mdata.reply;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import cn.hadoop.mdata.common.MysqlUtil;
import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.entity.DBDYReplyData;
import cn.hadoop.mdata.intermediate.Intermediate;
import cn.hadoop.mdata.queue.CommentFeeder;
import cn.hadoop.mdata.queue.Queue;
import cn.hadoop.mdata.task.DBCommentTasker;
import cn.moviebigdata.mdata.conf.MovieConfiguration;

public class CommentInject extends Configured implements Tool,MapRunnable<Text, DBDYData, Text, DBDYReplyData> {
	
	private static final Log LOG = LogFactory.getLog(CommentInject.class);
	
	private CommentFeeder feeder;
	private Queue<DBDYData> queue;
	
	AtomicInteger alivesum = new AtomicInteger(0);
	AtomicInteger spin_wait = new AtomicInteger(0); //默认为零
	AtomicInteger halt_sum = new AtomicInteger(0); //默认为零
	AtomicInteger fetch_total = new AtomicInteger(0); //默认为零
	
	private Set<String> fetched_mids;
	
	private Intermediate intermediate;
	
	@Override
	public void configure(JobConf conf) {
		setConf(conf);
		
	}

	@Override
	public void run(RecordReader<Text, DBDYData> reader,
			OutputCollector<Text, DBDYReplyData> collect, Reporter reporter)
			throws IOException {
		
		queue = new Queue<DBDYData>(getConf());
		try {
			fetched_mids = MysqlUtil.getInstance(getConf()).readfeched();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		feeder = new CommentFeeder(queue, reader, getConf(),fetched_mids);
		Thread feedThread = new Thread(feeder);
		feedThread.setDaemon(true);
		feedThread.setName("QueueFeeder");
		feedThread.start();
		int thread_num = Integer.parseInt(getConf().get("queen.thread.max",
				"10"));// 获取最大线程数
		String mediate_path = getConf().get("fetch.intermediate.file",
				"F:/moviedb/dbmoviedb/reply_fetched.txt");
//		fetched_mids = IntermediateReader.getInstance(mediate_path).read();
		
		if(fetched_mids == null){
			System.out.println("###############################尚未抓取过任何数据");
		}else{
			System.out.println("已经抓取的数据:\t" + fetched_mids.size());
		}
		intermediate = Intermediate.getIntanace(mediate_path);
		LOG.debug("init queue thread max sum :"
				+ getConf().get("queen.thread.max", "10"));
		//与此启动线程每个线程针对一个年份去抓取，直到将改年份数据抓取完为止，然后获取另一年份数据开始抓取
		for (int i = 0; i < thread_num; i++) {
			Thread tasker = new Thread(new DBCommentTasker(queue, feeder, collect,
					alivesum, spin_wait,halt_sum,fetch_total,intermediate,getConf()));
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
		
		intermediate.close();
		try {
			MysqlUtil.getInstance(getConf()).close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void inject(Path from,Path to) throws IOException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		System.out.println("CommentInject: starting at " + sdf.format(start));
		System.out.println("CommentInject: source: " + from);
		System.out.println("CommentInject: result: " + to);
		JobConf job = new JobConf(getConf());
		job.setJobName("CommentInject");
		FileInputFormat.addInputPath(job, from);
		FileOutputFormat.setOutputPath(job, to);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapRunnerClass(CommentInject.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DBDYReplyData.class);
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
		int res = ToolRunner.run(MovieConfiguration.create(), new CommentInject(),
				args);
		System.exit(res);
	}

}
