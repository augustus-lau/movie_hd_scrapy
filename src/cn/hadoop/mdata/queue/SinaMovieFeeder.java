package cn.hadoop.mdata.queue;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class SinaMovieFeeder implements Runnable {
	
	
	private static final Log LOG = LogFactory.getLog(SinaMovieFeeder.class);
	
	private Queue<String> queen;

	private int size;

	private boolean hasmore = true;

	public SinaMovieFeeder(Queue<String> queen, Configuration conf) {
		this.queen = queen;
		this.size = Integer.parseInt(conf.get("queue.max.size", "500"));
	}
	
	@Override
	public void run() {
		LOG.info("SinaMovieFeeder Aviliable : " + hasmore);
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
				while(hasmore && feed > 0){
					LOG.info("SinaMovieFeeder remain cache size: " + feed);
					for(int start = 2015; start >= 1990;start--){
						queen.putItem(start+"");
						feed--;
					}
					hasmore = false;
				}
			}
		}
	}
	public synchronized boolean isAvilable() {
		if (hasmore || queen.getQueueSize() > 0) {
			return true;
		}
		return false;
	}

}
