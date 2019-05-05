package cn.hadoop.mdata.queue;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;

/**
 * ץȡ�������۵������
 * @author Administrator
 *
 */
public class CommentFeeder implements Runnable{
	
	
	private static final Log LOG = LogFactory.getLog(CommentFeeder.class);

	private Queue<DBDYData> queen;

	private int size;

	private boolean hasmore = true;

	private RecordReader<Text, DBDYData> reader;
	
	private Set<String> fetched_mids;
	
	private int job_fetch_num;
	
	AtomicInteger step_fetched_num = new AtomicInteger(0);//��ȡץȡ��������
	
	
	public CommentFeeder(Queue<DBDYData> queen,
			RecordReader<Text, DBDYData> reader, Configuration conf,Set<String> fetched_mids) {
		this.queen = queen;
		this.reader = reader;
		this.fetched_mids = fetched_mids;
		this.size = Integer.parseInt(conf.get("queue.max.size", "500"));
		this.job_fetch_num = Integer.parseInt(conf.get("fetch.job.step.num", "1000"));
	}
	
	@Override
	public void run() {
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
					//��ʱ�Ƚ��Ƿ��Ѿ�ץȡ�����ε�����
					int hasremain = job_fetch_num - step_fetched_num.get();
					if(hasremain > 0){
						try {
							Text key = new Text();
							DBDYData _item = new DBDYData();
							hasmore = reader.next(key, _item);
							if(_item!=null){
								if(!StringUtils.isEmpty(_item.getMid()) && !"-".equals(_item.getMid())){
									if(fetched_mids!=null ){
										if(!fetched_mids.contains(_item.getMid())){
											queen.putItem(_item);
											step_fetched_num.incrementAndGet();
											feed--;
										}
									}else{
										queen.putItem(_item);
										step_fetched_num.incrementAndGet();
										feed--;
									}
								}
							}
						} catch (IOException e) {
							LOG.error("reader exception: " + e.getMessage());
							return;
						}
					}else{
						hasmore = false;
						break;
					}
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
