package cn.hadoop.mdata.task;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.queue.DBQueenFeeder;
import cn.hadoop.mdata.queue.Queue;
import cn.moviebigdata.mdata.parse.DBDYParseone;
import cn.moviebigdata.mdata.request.DBListRequest;
import cn.moviebigdata.mdata.request.Request;
import cn.moviebigdata.mdata.request.RequestFactory;
import cn.moviebigdata.mdata.request.Takser;

//�������߳���ִ�����
public class DBInjectTasker extends Takser {
	public static final Logger LOG = LoggerFactory.getLogger(DBInjectTasker.class);
	
	
	private Queue<String> queue;

	private DBQueenFeeder feeder;

	private OutputCollector<Text, DBDYData> output;

	private Request request;

	AtomicInteger alivesum, spin_wait, halt_sum, fetch_total;

	private boolean ishalt = false; // ��ʾ���߳��Ƿ���Ҫhalt��

	public DBInjectTasker(Queue<String> queue, DBQueenFeeder feeder,
			OutputCollector<Text, DBDYData> output, AtomicInteger alivesum,
			AtomicInteger spin_wait, AtomicInteger halt_sum,
			AtomicInteger fetch_total) {
		super();
		this.queue = queue;
		this.feeder = feeder;
		this.output = output;
		this.alivesum = alivesum;// ͳ�Ƶ�ǰ����ץȡ���ݵĻ�Ծ�߳���
		this.spin_wait = spin_wait;
		this.halt_sum = halt_sum;
		this.fetch_total = fetch_total;
	}

	@Override
	public void begin() {
		while (!ishalt) {
			LOG.info("[QUEENITEM:]\t" + "���ڻ�ȡ�����е����ݣ�");
			if (feeder.isAvilable()) {
				String base_url = queue.getItem();
				if (base_url != null) {
					this.alivesum.incrementAndGet();
					int start = 0;
					boolean end = false;
					// ���￪ʼ��������ݵ�����
					while (!end) {
						LOG.info("[PAGE:]\t" + "�����������ӵ�ַ��"+start);
						request = RequestFactory.createRequest(
								DBListRequest.class, base_url + start);
						String result = request.request();
						if (result != null && !"".equals(result)) {
							List<DBDYData> items = null;
							try {
								items = DBDYParseone
										.getResult(result);
							} catch (Exception e1) {
								LOG.error("[PARSE:]\t" + base_url + start+"\t[MSG:]\t" +e1.getMessage() +"\t[CONTENT:]\t"+result);
								continue;
							}
							if (items != null && items.size() > 0) {
								for (DBDYData temp : items) {
									if (Constant.FETCH_END_SIGNAL == temp
											.getFetch_status()) {
										end = true;
										start = 0;
										break;
									}
									temp.setFrompage(start+"");
									temp.setFetch_level(Constant.FETCH_LEVEL_INJECT);
									try {
										fetch_total.incrementAndGet();
										output.collect(new Text(temp.getMid()),
												temp);
									} catch (IOException e) {
										//���д�����ݳ�������Ӧ��ֹͣ���߳�
										end = true;
										ishalt = true;
										LOG.error("OutputCollector",start);
										break;
									}
								}
							}
						}
						start = start+15;
						// ���߳���Ϣ3�����
						try {
							Thread.sleep(3000);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					this.alivesum.decrementAndGet();
				} else {// ���û��ȡ�����ݣ����߳���Ϣ1��
					try {
						spin_wait.incrementAndGet();
						Thread.sleep(1000);
						spin_wait.decrementAndGet();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} else {
				ishalt = true;
				halt_sum.incrementAndGet();// ��¼halt�̵߳ĸ���
			}
		}
	}

}
