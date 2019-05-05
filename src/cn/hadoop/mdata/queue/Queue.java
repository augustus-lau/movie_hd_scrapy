package cn.hadoop.mdata.queue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * 之所以用泛型 ，是因为synchronized关键字是对 当前对象 加的锁
 * @author Administrator
 *
 * @param <E>
 */
public class Queue<E> {
	private static final Log LOG = LogFactory.getLog(Queue.class);
	
	public static final byte URL_UNDEEL = 0x01;  //连接尚未抓取
	public static final byte URL_FETCHED = 0x02; //连接已经抓取
	public static final byte URL_DEELED = 0x03;  //连接已经抓取，数据已经格式化，保存为hadoop的格式
	public static final byte URL_LINKS_FETCHED = 0x04;  //连接关联的详细信息已经抓取
	public static final byte URL_LINKS_DEELED = 0x05;   //连接关联的详细信息已经抓取，并且数据格式化完毕
	public static final byte URL_REPLY_FETCHED = 0x06;  //连接关联的回复信息尚未抓取
	public static final byte URL_REPLY_DEELED = 0x07;   //连接关联的回复信息已经抓取，并且数据格式化完毕
	public static final byte URL_REPLY_ERROR = 0x08; //表示抓取时报错
	private static final String QUEUE_MAX_SIZE="queue.max.size";
	
	private List<E> QUEUE_CACHE = Collections.synchronizedList(new LinkedList<E>());
	
	private int max_queen_size;
	
	public Queue(Configuration conf) {
		max_queen_size = Integer.parseInt(conf.get(QUEUE_MAX_SIZE, "500"));
		LOG.info("init queue max size :" + max_queen_size);
	}

	public synchronized boolean putItem(E e){
		if(!QUEUE_CACHE.contains(e)){
			QUEUE_CACHE.add(e);
			return true;
		}
		return false;
	}
	
	public synchronized E getItem(){
		if(QUEUE_CACHE.size()>0){
			E e = QUEUE_CACHE.get(0);
			QUEUE_CACHE.remove(0);
			return e;
		}
		return null;
	}
	
	 public synchronized int getQueueSize() {
		return QUEUE_CACHE.size();
	 }
}
