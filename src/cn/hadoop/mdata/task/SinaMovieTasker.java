package cn.hadoop.mdata.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import cn.hadoop.mdata.entity.MovieSinaJson;
import cn.hadoop.mdata.entity.SinaItem;
import cn.hadoop.mdata.queue.Queue;
import cn.hadoop.mdata.queue.SinaMovieFeeder;
import cn.moviebigdata.mdata.parse.plugin.ParseForSinaMovie;
import cn.moviebigdata.mdata.parse.plugin.ParseHtml;
import cn.moviebigdata.mdata.request.Takser;
import cn.moviebigdata.mdata.request.client.HttpPostRequest;
import cn.moviebigdata.mdata.request.client.RequestClient;
import cn.moviebigdata.mdata.response.ClientResponse;

public class SinaMovieTasker extends Takser{
	
	private static final Log logger = LogFactory.getLog(SinaMovieTasker.class);
	
	private Queue<String> queue;

	private SinaMovieFeeder feeder;

	private OutputCollector<Text, SinaItem> output;

	private RequestClient request;

	AtomicInteger alivesum, spin_wait, halt_sum, fetch_total;

	private boolean ishalt = false; // ��ʾ���߳��Ƿ���Ҫhalt��
	
	public SinaMovieTasker(Queue<String> queue, SinaMovieFeeder feeder,
			OutputCollector<Text, SinaItem> output, AtomicInteger alivesum,
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
				logger.info("���ڻ�ȡ�µĵ�ַ\t......");
				String  _year = queue.getItem();
				if (!StringUtils.isEmpty(_year)) {
					//��ʼץȡ
					this.alivesum.incrementAndGet();
					boolean hanextpage = true;//�ж��Ƿ�����һҳ
					String baseUrl = "http://movie.weibo.com/movie/webajax/category";
					request = new HttpPostRequest();
					int currentpage = 1;
					int sum_pre_year = 0;
					ParseHtml<MovieSinaJson> parse= null;
					MovieSinaJson _data = null;
					List<SinaItem> _datalist = null;
					while(hanextpage){
						List<NameValuePair> params = new ArrayList<NameValuePair>();
						params.add(new BasicNameValuePair("country", "0")); 
						params.add(new BasicNameValuePair("page", currentpage+""));
						params.add(new BasicNameValuePair("type", "0")); 
						params.add(new BasicNameValuePair("year", _year));
						ClientResponse reponse = request.request(baseUrl, params);
						if("200".equals(reponse.getCode())){
							parse = new ParseForSinaMovie();//��ȡ������
							try {
								_data = parse.parse(reponse.getContent());
								_datalist = _data.getData().getList();
							} catch (Exception e) {
								logger.info("������������\t" + e.getMessage() +"\t" +params.toString());
							}
							if(_datalist!=null && _datalist.size()>0){
								if(parse.hasNextPage(currentpage)){
									currentpage = parse.nextPage();
								}else{
									hanextpage = false;
//									logger.info(_year+"û����һҳ�����ˣ�\t" +currentpage);
								}
								try {
									for(SinaItem _sitem : _datalist){
										output.collect(new Text(_sitem.getFilm_id()), _sitem);
										sum_pre_year ++;
										fetch_total.incrementAndGet();
									}
								} catch (IOException e) {
									logger.info("д�����ݿ���̲������쳣��\t" + e.getMessage());
								}
							}else{
								if(parse.hasNextPage(currentpage)){
									break;
								}else{
//									logger.info("δ��������Ӧ������:\t"+baseUrl+params);
									hanextpage = false;
								}
								
							}
						}
					}
					logger.info(_year+"��ǰ��ݵĵ�Ӱ������:\t"+sum_pre_year);
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
