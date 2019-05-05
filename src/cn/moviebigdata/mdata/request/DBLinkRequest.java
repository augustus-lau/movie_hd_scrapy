package cn.moviebigdata.mdata.request;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class DBLinkRequest extends Request{
	
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
						resBuffer.append(resTemp);
				}
				String content = resBuffer.toString();
				if(content!=null && !"".equals(content)){
					//location.href="http://movie.douban.com/subject/25829320/"
					try{
						result = content.substring(content.indexOf("location.href")+15, content.indexOf(";'")-1);
					}catch (Exception e) {
						System.out.println(e.getMessage()+"\n" + content);
					}
				}
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
