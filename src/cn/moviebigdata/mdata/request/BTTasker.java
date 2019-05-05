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
 * 线程任务
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
	
	private boolean ishalt = false; //表示该线程是否需要halt掉
	
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
			//如果填充器是有效的，或者填充器停止了 但是队列中还有数据时
			if(feeder.isAvilable()){
				BTItem item = queue.getItem();
				//如果feeder是有效的 那么就从队列中取一条数据处理，如果取到数据，表示改线程已经接受了任务
				if(item != null){
					//线程记录为活跃线程
					alivesum.incrementAndGet();
					request = RequestFactory.createRequest(DBLinkRequest.class,item.getTodburl());
					String dburl=null;
					try {
						dburl = request.request();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					//将数据封装成hadoop的数据结构
					String mid = item.getUrl();
					mid = mid.substring(mid.indexOf("subject/")+8, mid.length()-5);
					BTData btdata = new BTData(mid,Queue.URL_LINKS_FETCHED,item.getMname(),item.getTodburl(),
							null,null,null,null,null,null,null,null,null,null,null,null);
					if(dburl!=null && !"".equals(dburl)){
						//处理数据，将数据输出
						btdata.setDburl(dburl);
						LOG.info("-fetching: \t todburl=" + item.getTodburl() + "\t dburl=" + dburl);
						//处理豆瓣的数据
						request = RequestFactory.createRequest(DBINFORequest.class,dburl);
						String dbresult=null;
						try {
							dbresult = request.request();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						deelDBData(btdata,dbresult);
						//添加点评地址
						btdata.setCommenturl(item.getUrl()+"/comments");
						//添加影评地址
						btdata.setReviewsurl(item.getUrl()+"/reviews");
					}
					try {
						output.collect(new Text(mid), new Text(btdata.toString()));
						alivesum.decrementAndGet();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}else{//如果接受到任务,记录线程的spinwait
					try {
						spin_wait.incrementAndGet();
						Thread.sleep(1000);
						spin_wait.decrementAndGet();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//让线程休息两秒再去接受任务
				try {
					spin_wait.incrementAndGet();
					Thread.sleep(5000);
					spin_wait.decrementAndGet();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else{ //如果填充器停止了,并且队列中没有数据了，那么该线程应该halt掉
				ishalt = true;
				halt_sum.incrementAndGet();//记录halt线程的个数
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
						btdata.setActorsarry(item.substring(2, item.length()).split("、"));
					}
					if(item.startsWith("G:") && item.length()>2){
						btdata.setGenresarry(item.substring(2, item.length()).split("、"));
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
