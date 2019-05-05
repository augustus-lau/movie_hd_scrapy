package cn.moviebigdata.mdata.request;

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

/**
 * �߳�����
 * @author liu_shuai
 *
 */
public class BTTasker extends Takser{
	private static final Log LOG = LogFactory.getLog(BTTasker.class);
	private Queue<BTItem> queue;
	
	private QueueFeeder feeder;
	
	private OutputCollector<Text, Text> output;
	
	private Request request;
	
	AtomicInteger alivesum ,spin_wait,halt_sum;
	
	private boolean ishalt = false; //��ʾ���߳��Ƿ���Ҫhalt��
	
	public BTTasker(Queue<BTItem> queue,QueueFeeder feeder,OutputCollector<Text, Text> output,
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
				BTItem item = queue.getItem();
				//���feeder����Ч�� ��ô�ʹӶ�����ȡһ�����ݴ������ȡ�����ݣ���ʾ���߳��Ѿ�����������
				if(item != null){
					//�̼߳�¼Ϊ��Ծ�߳�
					alivesum.incrementAndGet();
					request = RequestFactory.createRequest(DBLinkRequest.class,item.getTodburl());
					String dburl=null;
					try {
						dburl = request.request();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					//�����ݷ�װ��hadoop�����ݽṹ
					String mid = item.getUrl();
					mid = mid.substring(mid.indexOf("subject/")+8, mid.length()-5);
					BTData btdata = new BTData(mid,Queue.URL_LINKS_FETCHED,item.getMname(),item.getTodburl(),
							null,null,null,null,null,null,null,null,null,null,null,null);
					if(dburl!=null && !"".equals(dburl)){
						//�������ݣ����������
						btdata.setDburl(dburl);
						LOG.info("-fetching: \t todburl=" + item.getTodburl() + "\t dburl=" + dburl);
						//�����������
						request = RequestFactory.createRequest(DBINFORequest.class,dburl);
						String dbresult=null;
						try {
							dbresult = request.request();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						deelDBData(btdata,dbresult);
						//��ӵ�����ַ
						btdata.setCommenturl(item.getUrl()+"/comments");
						//���Ӱ����ַ
						btdata.setReviewsurl(item.getUrl()+"/reviews");
					}
					try {
						output.collect(new Text(mid), new Text(btdata.toString()));
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
	
	
	private void deelDBData(BTData btdata,String dbresult){
		if(dbresult!=null && !"".equals(dbresult)){
			String[] items = dbresult.substring(0, dbresult.length()-1).split("#");
			if(items.length>0){
				for(String item : items){
					if(item.startsWith("D:") && item.length()>2){
						btdata.setDirector(item.substring(2, item.length()));
					}
					if(item.startsWith("W:") && item.length()>2){
						btdata.setScriptwriter(item.substring(2, item.length()));
					}
					if(item.startsWith("A:") && item.length()>2){
						btdata.setActorsarry(item.substring(2, item.length()).split("��"));
					}
					if(item.startsWith("G:") && item.length()>2){
						btdata.setGenresarry(item.substring(2, item.length()).split("��"));
					}
					if(item.startsWith("U:") && item.length()>2){
						btdata.setCountry(item.substring(2, item.length()));
					}
					if(item.startsWith("T:") && item.length()>2){
						btdata.setRuntime(item.substring(2, item.length()));
					}
					if(item.startsWith("I:") && item.length()>2){
						btdata.setImdbmid(item.substring(2, item.length()));
					}
					if(item.startsWith("S:") && item.length()>2){
						btdata.setSorce(item.substring(2, item.length()));
					}
				}
			}
			
		}
	}
	
}
