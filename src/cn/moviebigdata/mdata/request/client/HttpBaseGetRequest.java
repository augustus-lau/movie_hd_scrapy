package cn.moviebigdata.mdata.request.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import cn.moviebigdata.mdata.request.ProxyGenerator;
import cn.moviebigdata.mdata.response.ClientResponse;

public class HttpBaseGetRequest extends RequestClient{

	private static final Log logger = LogFactory.getLog(HttpBaseGetRequest.class);
	
	private String[] agents = {"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0",
			"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
			"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
			"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
			"Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)",
			"Chrome/17.0.963.56 Safari/535.11",
			"Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko)"};
	
	BufferedReader br = null;
	URL url = null;
	
	int Max_ERROR_SUM = 0;
	
	@Override
	public ClientResponse request(String url_path, Object params) {
		ClientResponse result = null;
		HttpURLConnection connection = null;
		try {
			url = new URL(url_path);
			Properties prop = System.getProperties();
			String[] proxyUrl = ProxyGenerator.getInstance().generate();
			if(PROXY_IP_CACHE!=null){
				prop.setProperty("http.proxyHost", PROXY_IP_CACHE);
				prop.setProperty("http.proxyPort", PROXY_PORT_CACHE);
			}else if(proxyUrl!=null){
				prop.setProperty("http.proxyHost", proxyUrl[0]);
				prop.setProperty("http.proxyPort", proxyUrl[1]);
				PROXY_IP_CACHE = proxyUrl[0];
				PROXY_PORT_CACHE = proxyUrl[1];
			}
			
			Thread.sleep(1000);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setReadTimeout(READ_TIME_OUT);
			connection.setConnectTimeout(CONN_TIME_OUT);
			connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
			connection.setRequestProperty("Connection", "keep-alive");
			Random randomNum = new Random();
			connection.setRequestProperty("User-Agent",agents[randomNum.nextInt(agents.length)]); 
			connection.setRequestProperty("Host","movie.douban.com"); 
			connection.connect();
			result = new ClientResponse();
			if (connection.getResponseCode() == 200) {
				br = new BufferedReader(new InputStreamReader(
						connection.getInputStream(), "UTF-8"));
				
				StringBuffer resBuffer = new StringBuffer();
				String resTemp = "";
				while ((resTemp = br.readLine()) != null) {
					resBuffer.append(resTemp);
				}
				result.setContent(resBuffer.toString());
				result.setCode(200+"");
				result.setProtocol("HTTP");
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
				return result; 
			}else if(403 == connection.getResponseCode()){
				result.setCode(403+"");
				result.setProtocol("HTTP");
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("REQUESTCODE:\t403\n"+url +"\t 正在重新链接");
				return request(url_path,null);
			}else if(404 == connection.getResponseCode()){
				result.setCode(connection.getResponseCode()+"");
				result.setProtocol("HTTP");
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				return result;
			}else{
				result.setCode(connection.getResponseCode()+"");
				result.setProtocol("HTTP");
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("REQUESTCODE:\t"+connection.getResponseCode()+"\n"+url +"\t 正在重新链接");
				return request(url_path,null);
			}
			
		} catch(SocketTimeoutException te){
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(connection!=null){
				connection.disconnect();
			}
			
			if(Max_ERROR_SUM<5){
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("SocketTimeoutException:\t"+te.getMessage()+"\n"+url +"\t 正在重新链接");
				return request(url_path,null);
			}else{
				logger.info("链接已经损坏:\t"+te.getMessage());
				result.setCode("999999");
				result.setContent(te.getMessage());
				return result;
			}
		} catch(ConnectException ce){
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("ConnectException:\t"+ce.getMessage()+"\n"+url +"\t 正在重新链接");
			ce.printStackTrace();
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(connection!=null){
				connection.disconnect();
			}
			return request(url_path,null);
		} catch (SocketException se){ //捕获为读取完数据后的异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("SocketException:\t" +se.getMessage()+"\n"+url +"\t 正在重新链接");
			se.printStackTrace();
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(connection!=null){
				connection.disconnect();
			}
			return request(url_path,null);
		} catch (IOException io){ //经常性质的IO异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("IOException:\t" +io.getMessage()+"\n"+url +"\t 正在重新链接");
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(connection!=null){
				connection.disconnect();
			}
			return request(url_path,null);
		} catch (Exception e) {
			logger.info("未知异常:\t" + e.getMessage() +"\t" + url);
			result = new ClientResponse();
			result.setCode("999999");
			result.setContent(e.getMessage());
			if(br!=null){
				try {
					br.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			if(connection!=null){
				connection.disconnect();
			}
			return result;
		}
	}
	
}
