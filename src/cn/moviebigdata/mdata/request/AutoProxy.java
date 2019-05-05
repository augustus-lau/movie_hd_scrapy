package cn.moviebigdata.mdata.request;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AutoProxy {

	private String proxy_path = "http://www.xici.net.co/";

	public List<String> createProxy() {
		System.out.println("start loading proxys...");
		String result = "";
		BufferedReader br = null;
		boolean checkstart = false;
		URL url = null;
		HttpURLConnection connection = null;
		try {
			url = new URL(proxy_path);
			connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod("GET");
			connection.setReadTimeout(2000000);
			connection.setConnectTimeout(2000000);
			connection.setRequestProperty("Content-type",
					"application/x-www-form-urlencoded");
			connection.setRequestProperty("Content-Type",
					"text/plain; charset=UTF-8");
			connection.setRequestProperty("Connection", "keep-alive");
			connection.connect();
			if (connection.getResponseCode() == 200) {
				br = new BufferedReader(new InputStreamReader(
						connection.getInputStream(), "UTF-8"));
				StringBuffer resBuffer = new StringBuffer();
				String resTemp = "";
				while ((resTemp = br.readLine()) != null) {
					if (checkstart && resTemp.contains("QQ´úÀíIP")) {
						checkstart = false;
						break;
					}
					if (checkstart) {
						resBuffer.append(resTemp);
					}
					if (!checkstart
							&& resTemp
									.contains("<div id=\"home\" class=\"clearfix\">")) {
						checkstart = true;
						resBuffer.append(resTemp);
					}
				}
				result = resBuffer.toString();
				System.out.println("proxys load success!");
				return parseContent(result);
				
			} else {
				throw new Exception("ÇëÇó×´Ì¬Âð´íÎó");
			}
		} catch (Exception e) {
		} finally {
			connection.disconnect();
		}
		return null;
	}

	private List<String> parseContent(String content) {
		List<String> proxys = new ArrayList<String>();
		if (content != null && !"".equals(content)) {
			String tempStr = null;
			if (content != null) {
				Pattern p = Pattern.compile("\\s*|\t|\r|\n");
				Matcher m = p.matcher(content);
				tempStr = m.replaceAll("");
			}
			String titlereg = "<trclass=\"(.*?)\"><td><imgalt=\"(.*?)\"src=\"(.*?)\"/></td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td>HTTP</td><td>(.*?)</td></tr>";
			Pattern pattern = Pattern.compile(titlereg);
			Matcher matcher = pattern.matcher(tempStr);
			while(matcher.find()) {
				proxys.add(matcher.group(4)+":"+matcher.group(5));
			}
		}
		return proxys;
	}
	
	public static void main(String[] args) {
		List<String> proxys = new AutoProxy().createProxy();
		for(String proxy:proxys){
			System.out.println(proxy);
		}
	}
	
}
