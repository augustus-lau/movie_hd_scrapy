package cn.moviebigdata.mdata.request.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;

import cn.moviebigdata.mdata.request.ProxyGenerator;
import cn.moviebigdata.mdata.response.ClientResponse;

public class HttpPostRequest extends RequestClient {
	
	private static final Log logger = LogFactory.getLog(HttpPostRequest.class);
	
	@Override
	public ClientResponse request(String url ,Object params) {
		ClientResponse result = null;
		DefaultHttpClient httpclient = new DefaultHttpClient();
		
		//设置代理访问
		String[] proxyUrl = ProxyGenerator.getInstance().generate();
		if(PROXY_IP_CACHE!=null){
			HttpHost proxy = new HttpHost(PROXY_IP_CACHE, Integer.parseInt(PROXY_PORT_CACHE));
			httpclient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
		}else{
			PROXY_IP_CACHE = proxyUrl[0];
			PROXY_PORT_CACHE = proxyUrl[1];
			HttpHost proxy = new HttpHost(PROXY_IP_CACHE, Integer.parseInt(PROXY_PORT_CACHE));
			httpclient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
		}
		httpclient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, CONN_TIME_OUT);
		httpclient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, READ_TIME_OUT);
		HttpPost httppost = new HttpPost(url);
		httppost.addHeader("Accept", "application/json, text/javascript, */*; q=0.01");
		httppost.addHeader("Connection", "keep-alive");
		//这里一定要带这个，不然会自动填写java，这有很多网站会判断为爬虫程序了
		httppost.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; rv:39.0) Gecko/20100101 Firefox/39.0");
		httppost.addHeader("Referer", "http://movie.weibo.com/movie/web/category");
		httppost.addHeader("Host", "movie.weibo.com");
//		httppost.addHeader("Cookie", "UOR=www.dewen.io,widget.weibo.com,www.douban.com; SINAGLOBAL=1472967229432.6724.1418365621773; ULV=1437467128170:83:12:5:1655756105711.449.1437467128165:1437467108171; SUHB=0N2kBAm65834-C; __gads=ID=3ad537c3ca6a9728:T=1422342697:S=ALNI_MaHqixEofG3-gbmdLv9JtY1IAJEkw; __utma=15428400.98025819.1427082917.1427082917.1427082917.1; __utmz=15428400.1427082917.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none);SUBP=0033WrSXqPxfM72wWs9jqgMF55529P9D9WFX8TmeXoyyh3AWrH0Y0ETm5JpV8sDDdJUVIPHjIg8V9X5pSoeVqcv_; myuid=2694217733; SUB=_2AkMi8o4fdcNhrAFXkfwXzWzlb45TkFGq7oiodRKFRn1UXChJ3xAEx0lqtBN-Xtyh2Ue5wWYFC3UIY3LF-5SNOROcvF6BfeXPstoj; un=jingniuliang@163.com; NSC_JOt2otwdbb2ivlfddi4d3nempwbsmcc=ffffffff094111a145525d5f4f58455e445a4a423660; WBStore=8a7695f03b35652a|undefined");
		UrlEncodedFormEntity uefEntity = null;
		try {
			result = new ClientResponse();
			uefEntity = new UrlEncodedFormEntity((List<? extends NameValuePair>) params, "UTF-8");
			httppost.setEntity(uefEntity);
			HttpResponse response = httpclient.execute(httppost);
			
			if("200".equals(response.getStatusLine().getStatusCode()+"")){
				result.setCode(response.getStatusLine().getStatusCode()+"");
				result.setContent(EntityUtils.toString(response.getEntity(), "UTF-8"));
				result.setProtocol(response.getProtocolVersion().toString());
				httppost.releaseConnection();
				return result;
			}else{
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("REQUESTCODE:\t"+response.getStatusLine().getStatusCode()+"\n"+url +"\t 正在重新链接");
				return request(url,params);
			}
		} catch (UnsupportedEncodingException e) {//将参数转码的异常
			logger.error("未知异常:\t" + e.getMessage() +"\t" + url);
			result = new ClientResponse();
			result.setCode("999999");
			result.setContent(e.getMessage());
			return result;
		} catch(SocketTimeoutException te){
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("SocketTimeoutException:\t"+te.getMessage()+"\n"+url +"\t 正在重新链接");
			return request(url,params);
		} catch(ConnectException ce){
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("ConnectException:\t"+ce.getMessage()+"\n"+url +"\t 正在重新链接");
			return request(url,params);
		} catch (SocketException se){ //捕获为读取完数据后的异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("SocketException:\t" +se.getMessage()+"\n"+url +"\t 正在重新链接");
			return request(url,params);
		} catch (IOException io){ //经常性质的IO异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("IOException:\t" +io.getMessage()+"\n"+url +"\t 正在重新链接");
			return request(url,params);
		} catch (Exception e) {
			logger.error("未知异常:\t" + e.getMessage() +"\t" + url);
			result = new ClientResponse();
			result.setCode("999999");
			result.setContent(e.getMessage());
			return result;
		} 
	}

}
