package cn.hadoop.mdata.plugin;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import cn.hadoop.mdata.entity.BTData;
import cn.hadoop.mdata.entity.BTItem;
import cn.hadoop.mdata.queue.Queue;
import cn.hadoop.mdata.queue.QueueFeeder;
import cn.moviebigdata.mdata.request.BTTasker;
import cn.moviebigdata.mdata.request.DBINFORequest;
import cn.moviebigdata.mdata.request.DBLinkRequest;
import cn.moviebigdata.mdata.request.Request;
import cn.moviebigdata.mdata.request.RequestFactory;
import cn.moviebigdata.mdata.request.Takser;

public class BTReplyTasker extends Takser{
	
	private static final Log LOG = LogFactory.getLog(BTTasker.class);
	
	private Queue<BTData> queue;
	
	private QueueFeeder feeder;
	
	private OutputCollector<Text, Text> output;
	
	private Request request;
	
	AtomicInteger alivesum ,spin_wait,halt_sum;
	
	private boolean ishalt = false; //��ʾ���߳��Ƿ���Ҫhalt��
	
	public BTReplyTasker(Queue<BTData> queue,QueueFeeder feeder,OutputCollector<Text, Text> output,
			AtomicInteger alivesum,AtomicInteger spin_wait,AtomicInteger halt_sum) {
		super();
		this.queue = queue;
		this.feeder = feeder;
		this.output = output;
		this.alivesum = alivesum;
		this.spin_wait = spin_wait;
		this.halt_sum = halt_sum;
	}
	
	
	@Override
	public void begin() {
		while(!ishalt){
			//������������Ч�ģ����������ֹͣ�� ���Ƕ����л�������ʱ
			if(feeder.isAvilable()){
				BTData item = queue.getItem();
				//���feeder����Ч�� ��ô�ʹӶ�����ȡһ�����ݴ������ȡ�����ݣ���ʾ���߳��Ѿ�����������
				if(item != null){
					//�̼߳�¼Ϊ��Ծ�߳�
					alivesum.incrementAndGet();
					request = RequestFactory.createRequest(DBLinkRequest.class,item.getDburl());
					String dburl=null;
					try {
						dburl = request.request();
					} catch (Exception e1) {
						//���쳣����
						
					}
					//�����ݷ�װ��hadoop�����ݽṹ
					
					try {
						output.collect(new Text(item.getMid()), new Text(item.toString()));
						alivesum.decrementAndGet();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{//������ܵ�����,��¼�̵߳�spinwait
					try {
						spin_wait.incrementAndGet();
						Thread.sleep(1000);
						spin_wait.decrementAndGet();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//���߳���Ϣ������ȥ��������
				try {
					spin_wait.incrementAndGet();
					Thread.sleep(5000);
					spin_wait.decrementAndGet();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else{ //��������ֹͣ��,���Ҷ�����û�������ˣ���ô���߳�Ӧ��halt��
				ishalt = true;
				halt_sum.incrementAndGet();//��¼halt�̵߳ĸ���
			}
		}
	}

}
