package cn.hadoop.mdata.task;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.queue.DBFillingFeeder;
import cn.hadoop.mdata.queue.Queue;
import cn.moviebigdata.mdata.parse.DBDYFillingParse;
import cn.moviebigdata.mdata.request.DBINFORequest;
import cn.moviebigdata.mdata.request.Request;
import cn.moviebigdata.mdata.request.RequestFactory;
import cn.moviebigdata.mdata.request.Takser;

public class DBFillingTasker extends Takser{
	
	private static final Log logger = LogFactory.getLog(DBFillingTasker.class);
	
	private Queue<DBDYData> queue;

	private DBFillingFeeder feeder;

	private OutputCollector<Text, DBDYData> output;

	private Request request;

	AtomicInteger alivesum, spin_wait, halt_sum, fetch_total;

	private boolean ishalt = false; // ��ʾ���߳��Ƿ���Ҫhalt��
	
	public DBFillingTasker(Queue<DBDYData> queue, DBFillingFeeder feeder,
			OutputCollector<Text, DBDYData> output, AtomicInteger alivesum,
			AtomicInteger spin_wait, AtomicInteger halt_sum,
			AtomicInteger fetch_total) {
		super();
		this.queue = queue;
		this.feeder = feeder;
		this.output = output;
		this.alivesum = alivesum; // ͳ�Ƶ�ǰ����ץȡ���ݵĻ�Ծ�߳���
		this.spin_wait = spin_wait;
		this.halt_sum = halt_sum;
		this.fetch_total = fetch_total;
	}
	
	@Override
	public void begin() {
		while (!ishalt) {
			if (feeder.isAvilable()) {
				DBDYData _item = queue.getItem();
				if (_item != null && !StringUtils.isEmpty(_item.getMovieurl())) {
					//��ʼץȡ
					this.alivesum.incrementAndGet();
					_item.setFetch_level(Constant.FETCH_LEVEL_FILLING);//�ı�����Level
					_item.setTimestamp(System.currentTimeMillis()+"");
					request = RequestFactory.createRequest(
							DBINFORequest.class, _item.getMovieurl());
					String result = request.request();
					try {
						new DBDYFillingParse().parse(_item, result);
					} catch (Exception e) {
						_item.setFetch_status(Constant.FETCH_PARSE_ERROR);
						_item.setFetch_level(Constant.FETCH_LEVEL_FILLING);
						_item.setFetch_msg(Constant.FETCH_MSG_ERROR);
						logger.info("�������̲������쳣��\t" + e.getMessage());
					}
					try {
						output.collect(new Text(_item.getMid()), _item);
					} catch (IOException e) {
						logger.info("д�����ݿ���̲������쳣��\t" + e.getMessage());
					}
					fetch_total.incrementAndGet();
					this.alivesum.decrementAndGet();
					// ���߳���Ϣ3�����
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}else{
					try {
						spin_wait.incrementAndGet();
						Thread.sleep(1000);
						spin_wait.decrementAndGet();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
			}else{
				ishalt = true;
				halt_sum.incrementAndGet();// ��¼halt�̵߳ĸ���
			}
		}
	}

}
