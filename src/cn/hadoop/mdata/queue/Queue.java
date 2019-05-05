package cn.hadoop.mdata.queue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * ֮�����÷��� ������Ϊsynchronized�ؼ����Ƕ� ��ǰ���� �ӵ���
 * @author Administrator
 *
 * @param <E>
 */
public class Queue<E> {
	private static final Log LOG = LogFactory.getLog(Queue.class);
	
	public static final byte URL_UNDEEL = 0x01;  //������δץȡ
	public static final byte URL_FETCHED = 0x02; //�����Ѿ�ץȡ
	public static final byte URL_DEELED = 0x03;  //�����Ѿ�ץȡ�������Ѿ���ʽ��������Ϊhadoop�ĸ�ʽ
	public static final byte URL_LINKS_FETCHED = 0x04;  //���ӹ�������ϸ��Ϣ�Ѿ�ץȡ
	public static final byte URL_LINKS_DEELED = 0x05;   //���ӹ�������ϸ��Ϣ�Ѿ�ץȡ���������ݸ�ʽ�����
	public static final byte URL_REPLY_FETCHED = 0x06;  //���ӹ����Ļظ���Ϣ��δץȡ
	public static final byte URL_REPLY_DEELED = 0x07;   //���ӹ����Ļظ���Ϣ�Ѿ�ץȡ���������ݸ�ʽ�����
	public static final byte URL_REPLY_ERROR = 0x08; //��ʾץȡʱ����
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
