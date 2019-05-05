package cn.moviebigdata.mdata.request;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 这是豆瓣电影按照年份显示列表页面的访问
 * 如果正方返回状态吗200：这返回的结果为SUCCESS:+RESULT
 * 否则返回的结果为ERRORCODE:CODE[:ECODE-END]
 * 如果是数据异常 则返回ERRORMSG：开头，[:EMSG-END]结尾
 * 如果结果集返回的内容是HALT:开头，表示已经没有相关数据
 * @author Administrator
 *
 */
public class DBListRequest extends Request{

	public static final Logger LOG = LoggerFactory.getLogger(DBListRequest.class);
	
	@Override
	public String request() {
		String result = null;
		BufferedReader br = null;
		boolean isstart = false;
		URL url = null;
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
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setReadTimeout(READ_TIME_OUT);
			connection.setConnectTimeout(CONN_TIME_OUT);
			connection.setRequestProperty("Content-Type",
					"text/plain; charset=UTF-8");
			connection.setRequestProperty("Connection", "keep-alive");
			connection.setRequestProperty("User-agent","Slurp"); 
			connection.connect();
			LOG.info("正在链接:\t"+url_path);
			if (connection.getResponseCode() == 200) {
				br = new BufferedReader(new InputStreamReader(
						connection.getInputStream(), "UTF-8"));
				StringBuffer resBuffer = new StringBuffer();
				String resTemp = "";
				while ((resTemp = br.readLine()) != null) {
					if(resTemp.contains("<div class=\"paginator\">") && isstart){
						isstart = false;
						break;
					}
					if(resTemp.contains("<div class=\"mod movie-list\">") || isstart){
						resBuffer.append(resTemp);
						isstart = true;
					}
				}
				String temp = resBuffer.toString();
				if(temp.indexOf("<dt>")<=0){
					result = "HALT:已经没有数据了";
				}else{
					result = "SUCCESS:"+resBuffer.toString();
				}
			}else{
				result = "ERRORCODE:"+connection.getResponseCode()+"[:ECODE-END]";
			}
			if(br!=null){
				br.close();
			}
			if(connection!=null){
				connection.disconnect();
			}
			return result;
		
		
		} catch(SocketTimeoutException te){
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			LOG.error("SocketTimeoutException:\t"+te.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request();
		} catch(ConnectException ce){
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			LOG.error("ConnectException:\t"+ce.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request();
		} catch (SocketException se){ //捕获为读取完数据后的异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			LOG.error("SocketException:\t" +se.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request();
		} catch (Exception e) {
			e.printStackTrace();
			return "ERRORMSG:"+e.getMessage()+"[:EMSG-END]";
		} finally {
		}
	}

}
