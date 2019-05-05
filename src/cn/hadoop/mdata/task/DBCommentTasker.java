package cn.hadoop.mdata.task;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.common.MysqlUtil;
import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.entity.DBDYReplyData;
import cn.hadoop.mdata.exception.NoneFetchItemException;
import cn.hadoop.mdata.intermediate.Intermediate;
import cn.hadoop.mdata.queue.CommentFeeder;
import cn.hadoop.mdata.queue.Queue;
import cn.moviebigdata.mdata.parse.plugin.ParseForDBReply;
import cn.moviebigdata.mdata.parse.plugin.ParseHtml;
import cn.moviebigdata.mdata.request.Takser;
import cn.moviebigdata.mdata.request.client.HttpBaseGetRequest;
import cn.moviebigdata.mdata.request.client.RequestClient;
import cn.moviebigdata.mdata.response.ClientResponse;

public class DBCommentTasker extends Takser{
	
	
	private static final Log logger = LogFactory.getLog(DBCommentTasker.class);
	
	private Queue<DBDYData> queue;

	private CommentFeeder feeder;

	private OutputCollector<Text, DBDYReplyData> output;

	private RequestClient request;

	AtomicInteger alivesum, spin_wait, halt_sum, fetch_total;

	private boolean ishalt = false; // ��ʾ���߳��Ƿ���Ҫhalt��
	
	private Intermediate intermediate;
	
	private Configuration conf;
	
	public DBCommentTasker(Queue<DBDYData> queue, CommentFeeder feeder,
			OutputCollector<Text, DBDYReplyData> output, AtomicInteger alivesum,
			AtomicInteger spin_wait, AtomicInteger halt_sum,
			AtomicInteger fetch_total,Intermediate intermediate,Configuration conf) {
		super();
		this.queue = queue;
		this.feeder = feeder;
		this.output = output;
		this.alivesum = alivesum; // ͳ�Ƶ�ǰ����ץȡ���ݵĻ�Ծ�߳���
		this.spin_wait = spin_wait;
		this.halt_sum = halt_sum;
		this.fetch_total = fetch_total;
		this.intermediate = intermediate;
		this.conf = conf;
	}
	
	
	@Override
	public void begin() {
		while (!ishalt) {
			if (feeder.isAvilable()) {
				DBDYData _item = queue.getItem();
				if (_item != null && !StringUtils.isEmpty(_item.getMid())) {
					//��ʼץȡ
					logger.info("���ڻ�ȡ�µĵ�ַ\t......" + _item.getMid());
					this.alivesum.incrementAndGet();
					_item.setFetch_level(Constant.FETCH_LEVEL_REPLY);//�ı�����Level
					_item.setTimestamp(System.currentTimeMillis()+"");
					boolean hanextpage = true;//�ж��Ƿ�����һҳ
					String baseUrl = "http://movie.douban.com/subject/"+_item.getMid()+"/comments?start=";
					request = new HttpBaseGetRequest();
					String params = "&limit=20&sort=new_score";
					int curpage = 0;
					while(hanextpage){
						ClientResponse reponse = request.request(baseUrl+curpage+params, null);
						if("200".equals(reponse.getCode())){
							ParseHtml<DBDYReplyData> parse = new ParseForDBReply();//��ȡ������
							List<DBDYReplyData> re_items = null;
							try {
								re_items = parse.parsetoList(reponse.getContent(),_item.getMid());
							} catch (NoneFetchItemException e){//����������
								hanextpage = false;
								logger.info("ץȡ���ݲ�����\t" +baseUrl+curpage+params );
								break;
							} catch (Exception e) {
								logger.info("������������\t" + e.getMessage() +"\t" +baseUrl+curpage+params);
								hanextpage = false;
								break;
							}
							if(re_items!=null && re_items.size()>0){
								if(parse.hasNextPage(curpage)){
									curpage = parse.nextPage();
								}else{
									hanextpage = false;
								}
								try {
									for(DBDYReplyData data : re_items){
										output.collect(new Text(_item.getMid()), data);
										fetch_total.incrementAndGet();
									}
									try {
										MysqlUtil.getInstance(conf).write(re_items);
									} catch (SQLException e) {
										logger.info("д��mysql���̲������쳣��\t" + e.getMessage());
									}
								} catch (IOException e) {
									logger.info("д��fsdb���̲������쳣��\t" + e.getMessage());
								}
							}else{
								logger.info("δ��������Ӧ������:\t"+baseUrl+params);
							}
						}else if("999999".equals(reponse.getCode())){
							hanextpage = false;
						}else if("404".equals(reponse.getCode())){
							hanextpage = false;
						}
					}
					
					try {
						intermediate.write(_item.getMid());
					} catch (IOException e1) {
						logger.info("��¼��־�����쳣\t" + e1.getMessage());
					}
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
