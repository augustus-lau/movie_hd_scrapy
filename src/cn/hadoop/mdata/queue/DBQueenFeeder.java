package cn.hadoop.mdata.queue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
public class DBQueenFeeder implements Runnable{
	
	private static final Log LOG = LogFactory.getLog(QueueFeeder.class);
	
	private Queue<String> queen;
	
	private int size;
	
	private boolean hasmore = true;
	
	private RecordReader<LongWritable, Text> reader;
	
	
	public DBQueenFeeder(Queue<String> queen,RecordReader<LongWritable, Text> reader,Configuration conf){
		this.queen = queen;
		this.reader = reader;
		this.size = Integer.parseInt(conf.get("queue.max.size", "500"));
	}
	
	@Override
	public void run() {
		LOG.debug("QueueFeeder Aviliable : " + hasmore);
		while(hasmore){
			int feed = size-queen.getQueueSize(); //��ʾ���ڶ��е�ʣ��ռ�
			if(feed <= 0){//�����Ѿ�����
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 continue;
			}else{
				while(hasmore && feed >0){
					LOG.info("QueueFeeder remain cache size: " + feed);
					try {
						LongWritable key = new LongWritable();
						Text url_page = new Text();
						hasmore = reader.next(key, url_page);
						int startyear = 2015;
						int endyear = 1989;
						do{
							queen.putItem("http://www.douban.com/tag/"+startyear+"/movie?start=");
							feed--;
							startyear--;
						}while(startyear-endyear>=0);
						
					} catch (IOException e) {
						LOG.error("reader exception: " + e.getMessage());
						return;
					}
				}
			}
		}
		LOG.info("feed �Ѿ���������.... \t ��ǰQueue�еĻ�������Ϊ��" +queen.getQueueSize());
	}
	public synchronized boolean isAvilable(){
		if(hasmore || queen.getQueueSize()>0){
			return true;
		}
		return false;
	}
	
}
