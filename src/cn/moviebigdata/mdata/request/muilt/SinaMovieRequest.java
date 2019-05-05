package cn.moviebigdata.mdata.request.muilt;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.moviebigdata.mdata.request.ProxyGenerator;


/**
 * post 请求http://movie.weibo.com/movie/webajax/category
 * type
 * country
 * year
 * page
 * @author tk
 *
 */
public class SinaMovieRequest{

	private static final Log logger = LogFactory.getLog(SinaMovieRequest.class);
	
	public int READ_TIME_OUT=60*1000*2;
	public int CONN_TIME_OUT=60*1000*2;
	public String PROXY_IP_CACHE;
	public String PROXY_PORT_CACHE;
	
	public String request(String url_path , String params) {
		String result = null;
		BufferedReader br = null;
		URL url = null;
		HttpURLConnection connection = null;
		try {
			url = new URL(url_path);
			Properties prop = System.getProperties();
			String[] proxyUrl = ProxyGenerator.getInstance().generate();
			if(PROXY_IP_CACHE!=null){
				prop.setProperty("http.proxyHost", PROXY_IP_CACHE);
				prop.setProperty("http.proxyPort", PROXY_PORT_CACHE);
			}else{
				prop.setProperty("http.proxyHost", proxyUrl[0]);
				prop.setProperty("http.proxyPort", proxyUrl[1]);
				PROXY_IP_CACHE = proxyUrl[0];
				PROXY_PORT_CACHE = proxyUrl[1];
			}
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod("POST");
			connection.setUseCaches(false);
			connection.setReadTimeout(READ_TIME_OUT);
			connection.setConnectTimeout(CONN_TIME_OUT);
			connection.setRequestProperty("Connection", "keep-alive");
			connection.setRequestProperty("Accept","application/json, text/javascript, */*; q=0.01");
			connection.setRequestProperty("Accept-Language","zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3");
			connection.setRequestProperty("Accept-Encoding","gzip, deflate");
			connection.setRequestProperty("Content-Type","application/x-www-form-urlencoded; charset=UTF-8");
			connection.setRequestProperty("X-Requested-With","XMLHttpRequest");
			connection.setRequestProperty("Cookie","UOR=fight.pcgames.com.cn,widget.weibo.com,www.iqiyi.com; SUBP=0033WrSXqPxfM72wWs9jqgMF55529P9D9WFX8TmeXoyyh3AWrH0Y0ETm5JpX5KMt; SINAGLOBAL=7994307563783.166.1435160857622; ULV=1437525272687:6:4:2:1242994652322.5698.1437525272673:1437480449020; SUHB=08JqlWJ8I8ImnR; wvr=6; SUB=_2AkMi8rksdcNhrAFXkfwXzWzlb45H-jjGieTAAX_rJhIxcEF-7adZ0YcYoL-tou7E-7AzBaG9reoP; NSC_JOt2otwdbb2ivlfddi4d3nempwbsmcc=ffffffff094111bd45525d5f4f58455e445a4a423660; WBStore=8a7695f03b35652a|undefined"); 
			connection.connect();
			
			DataOutputStream out = new DataOutputStream(connection
	                .getOutputStream());
			out.writeUTF(params);
	        out.flush();
	        out.close(); 
			
			if (connection.getResponseCode() == 200) {
				br = new BufferedReader(new InputStreamReader(
						connection.getInputStream(), "UTF-8"));
				StringBuffer resBuffer = new StringBuffer();
				String resTemp = "";
				while ((resTemp = br.readLine()) != null) {
					resBuffer.append(resTemp);
				}
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
				result = "SUCCESS="+resBuffer.toString();
				return result;
			}else{
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("[RCODE:]\t"+connection.getResponseCode()+"\t"+url_path);
				return request(url_path,params);
			}
			
		} catch(SocketTimeoutException te){
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("SocketTimeoutException:\t"+te.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request(url_path,params);
		} catch(ConnectException ce){
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("ConnectException:\t"+ce.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request(url_path,params);
		} catch (SocketException se){ //捕获为读取完数据后的异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("SocketException:\t" +se.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request(url_path,params);
		} catch (Exception e) {
			e.printStackTrace();
			return "ERRORMSG:"+e.getMessage()+"[:EMSG-END]";
		} finally {
		}
	}

}
