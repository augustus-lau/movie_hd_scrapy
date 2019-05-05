package cn.moviebigdata.mdata.request.muilt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import cn.moviebigdata.mdata.request.ProxyGenerator;
import cn.moviebigdata.mdata.request.Request;

public class SinaUserRequest extends Request {

	private static final Log logger = LogFactory.getLog(SinaUserRequest.class);

	
	@Override
	public String request() {

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
			connection.setRequestMethod("GET");
			connection.setReadTimeout(READ_TIME_OUT);
			connection.setConnectTimeout(CONN_TIME_OUT);
			connection.setRequestProperty("Content-Type",
					"text/plain; charset=UTF-8");
			connection.setRequestProperty("Connection", "keep-alive");
			connection.setRequestProperty("User-agent","Slurp"); 
			connection.connect();
			if (connection.getResponseCode() == 200) {
				br = new BufferedReader(new InputStreamReader(
						connection.getInputStream(), "UTF-8"));
				StringBuffer resBuffer = new StringBuffer();
				String resTemp = "";
				while ((resTemp = br.readLine()) != null) {
					if(resTemp.contains("Pl_Core_UserInfo__5")){
						resBuffer.append(resTemp);
					}
				}
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
				result = resBuffer.toString();
				return result;
			}else{
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("[RCODE:]\t"+connection.getResponseCode()+"\t"+url_path);
				return request();
			}
			
		} catch(SocketTimeoutException te){
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("SocketTimeoutException:\t"+te.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request();
		} catch(ConnectException ce){
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("ConnectException:\t"+ce.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request();
		} catch (SocketException se){ //捕获为读取完数据后的异常
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.error("SocketException:\t" +se.getMessage()+"\n"+url_path +"\t 正在重新链接");
			return request();
		} catch (Exception e) {
			e.printStackTrace();
			return "ERRORMSG:"+e.getMessage()+"[:EMSG-END]";
		} finally {
		}
	}

}
