package cn.hadoop.mdata.queue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import cn.hadoop.mdata.entity.BTItem;

/**
 * 队列填充器
 * @author Administrator
 *
 */
public class QueueFeeder implements Runnable {
	
	private static final Log LOG = LogFactory.getLog(QueueFeeder.class);
	
	private Queue<BTItem> queen;
	
	private int size;
	
	private boolean hasmore = true;
	
	private RecordReader<LongWritable, Text> reader;
	
	public QueueFeeder(Queue<BTItem> queen,RecordReader<LongWritable, Text> reader,Configuration conf){
		this.queen = queen;
		this.reader = reader;
		this.size = Integer.parseInt(conf.get("queue.max.size", "500"));
	}
	
	@Override
	public void run() {
		LOG.debug("QueueFeeder Aviliable : " + hasmore);
		while(hasmore){
			int feed = size-queen.getQueueSize(); //表示现在队列的剩余空间
			if(feed <= 0){//队列已经满了
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
						LongWritable linenum = new LongWritable();
						Text content = new Text();
						hasmore = reader.next(linenum, content);
						BTItem item = parseToItem(content.toString());
						queen.putItem(item);
						feed--;
					} catch (IOException e) {
						LOG.error("reader exception: " + e.getMessage());
						return;
					}
				}
			}
		}
	}
	
	public synchronized boolean isAvilable(){
		if(hasmore || queen.getQueueSize()>0){
			return true;
		}
		return false;
	}
	
	private BTItem parseToItem(String content){
		BTItem item = new BTItem();
		if(content!=null && !"".equals(content)){
			String[] attrs = content.split("\t");
			item.setUrl(attrs[1]);
			String[] nameStr = attrs[2].split("/");
			if(nameStr.length<=1){
				item.setMname(attrs[2]);
			}else if(nameStr.length==2){
				item.setMname(nameStr[0]);
				item.setOname(nameStr[1]);
			}else{
				item.setMname(attrs[2]);
			}
			item.setTodburl(attrs[3]);
			item.setFetchStatus(Queue.URL_FETCHED);
			return item;
		}
		return null;
	}
}
