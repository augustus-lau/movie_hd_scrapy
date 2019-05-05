package cn.hadoop.mdata.plugin;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import cn.moviebigdata.mdata.request.ProxyGenerator;
import cn.moviebigdata.mdata.request.Request;

public class BTReplyRequest extends Request {

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
					if(resTemp.contains("<span class=\"fleft\">")){
						isstart = true;
					}
				}
				result = resBuffer.toString();
			}else{
				System.out.println("[RCODE:]\t"+connection.getResponseCode()+"\t"+url_path);
			}
			if(br!=null){
				br.close();
			}
			if(connection!=null){
				connection.disconnect();
			}
		} catch (Exception e) {
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			e.printStackTrace();
		} finally {
			if(connection!=null){
				connection.disconnect();
				connection=null;
			}
		}
		return result;
	}

}
