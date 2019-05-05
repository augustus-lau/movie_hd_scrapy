package cn.hadoop.mdata.queue;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;

public class DBFillingFeeder implements Runnable {

	private static final Log LOG = LogFactory.getLog(DBFillingFeeder.class);

	private Queue<DBDYData> queen;

	private int size;

	private boolean hasmore = true;

	private RecordReader<Text, DBDYData> reader;

	public DBFillingFeeder(Queue<DBDYData> queen,
			RecordReader<Text, DBDYData> reader, Configuration conf) {
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
						Text key = new Text();
						DBDYData _item = new DBDYData();
						hasmore = reader.next(key, _item);
						if(_item!=null){
							String mid = _item.getMid();
							byte fetch_status = _item.getFetch_status();
							byte level = _item.getFetch_level();
							if(!StringUtils.isEmpty(mid) && !mid.equals("-") 
									&& fetch_status ==0 && level ==Constant.FETCH_LEVEL_INJECT){
								queen.putItem(_item);
								feed--;
							}
						}
					} catch (IOException e) {
						LOG.error("reader exception: " + e.getMessage());
						return;
					}
				}
			}
		}
		LOG.info("feed 已经结束工作.... \t 当前Queue中的缓存数量为：" +queen.getQueueSize());
	}

	public synchronized boolean isAvilable() {
		if (hasmore || queen.getQueueSize() > 0) {
			return true;
		}
		return false;
	}

}
