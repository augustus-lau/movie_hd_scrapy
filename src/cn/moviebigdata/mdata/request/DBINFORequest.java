package cn.moviebigdata.mdata.request;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 这是豆瓣电影详情页面的数据访问
 * @author Administrator
 *
 */
public class DBINFORequest extends Request {
	
	private static final Log logger = LogFactory.getLog(DBINFORequest.class);
	
	@Override
	public String request() {
		String result = null;
		BufferedReader br = null;
		boolean isstart = false;
		boolean substart = false;
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
			if (connection.getResponseCode() == 200) {
				br = new BufferedReader(new InputStreamReader(
						connection.getInputStream(), "UTF-8"));
				StringBuffer resBuffer = new StringBuffer();
				StringBuffer subBuffer = new StringBuffer();
				String resTemp = "";
				while ((resTemp = br.readLine()) != null) {
					if(substart && resTemp.contains("</span>")){
						substart = false;
					}
					if(isstart && resTemp.contains("id=\"interest_sectl\"")){
						isstart = false;
					}
					if(resTemp.contains("<div id=\"info\">")){
						isstart = true;
					}
					if(resTemp.contains("property=\"v:summary\"")){
						substart = true;
					}
					
					if(substart){
						subBuffer.append(resTemp);
					}
					
					if(resTemp.contains("comments?sort=new_score")){
						resBuffer.append(parseRelyCount(resTemp)).append("#");
					}
					if(resTemp.contains("rel=\"v:image\"")){
						resBuffer.append(parseImage(resTemp)).append("#");
					}
					if(resTemp.contains("class=\"ll rating_num\"")){
						resBuffer.append(parseScore(resTemp)).append("#");
					}

					if(isstart){
						if(resTemp.contains("<span class='pl'>导演</span>")){
							resBuffer.append(parseDirector(resTemp)).append("#");
						}
						if(resTemp.contains("<span class='pl'>编剧</span>")){
							resBuffer.append(parseWriter(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"actor\">")){
							resBuffer.append(parseActors(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"pl\">类型:</span>")){
							resBuffer.append(parseGenre(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"pl\">制片国家/地区:</span>")){
							resBuffer.append(parseUnion(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"pl\">语言:</span>")){
							resBuffer.append(parseYuYan(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"pl\">上映日期:</span>")){
							resBuffer.append(parsePubDate(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"pl\">片长:</span>")){
							resBuffer.append(parseTime(resTemp)).append("#");
						}
						if(resTemp.contains("<span class=\"pl\">IMDb链接:</span>")){
							resBuffer.append(parseIMDB(resTemp)).append("#");
						}
					}
				}
				String summary = parseAbstract(subBuffer.toString());
				result = "SUCCESS="+resBuffer.toString()+summary;
				logger.info("[SUCCESS:]"+resBuffer.toString()+summary);
				if(br!=null){
					br.close();
				}
				if(connection!=null){
					connection.disconnect();
				}
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
	
	//解析导演
	private String parseDirector(String content){
		String result = "D:";
		String directe_regax = "<a href=\"(.*?)\" rel=\"v:directedBy\">(.*?)</a>";
		Pattern pattern = Pattern.compile(directe_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(2) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	
	//解析编剧
	private String parseWriter(String content){
		String result = "W:";
		String writer_regax = "<a href=\"(.*?)\">(.*?)</a>";
		Pattern pattern = Pattern.compile(writer_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(2) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	
	//解析演员
	private String parseActors(String content){
		String result = "A:";
		String actors_regax = "<a href=\"(.*?)\">(.*?)</a>";
		Pattern pattern = Pattern.compile(actors_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(2) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析类型
	private String parseGenre(String content){
		String result = "G:";
		String genre_regax = "<span property=\"v:genre\">(.*?)</span>";
		Pattern pattern = Pattern.compile(genre_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(1) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析国家
	private String parseUnion(String content){
		String result = "U:";
		String union_regax = "<span class=\"pl\">制片国家/地区:</span>(.*?)<br/>";
		Pattern pattern = Pattern.compile(union_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(1) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析语言
	private String parseYuYan(String content){
		String result = "Y:";
		String yuyan_regax = "<span class=\"pl\">语言:</span>(.*?)<br/>";
		Pattern pattern = Pattern.compile(yuyan_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(1) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析片长
	private String parseTime(String content){
		String result = "T:";
		String time_regax = "<span property=\"v:runtime\" content=\"(.*?)\">(.*?)</span>";
		Pattern pattern = Pattern.compile(time_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(2) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析IMDB链接
	private String parseIMDB(String content){
		String result = "I:";
		String IMDB_regax = "<a href=\"(.*?)\" target=\"_blank\" rel=\"(.*?)\">(.*?)</a>";
		Pattern pattern = Pattern.compile(IMDB_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(3) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	
	//解析评分
	private String parseScore(String content){
		String result = "S:";
		String score_regax = "<strong class=\"ll rating_num\" property=\"v:average\">(.*?)</strong>";
		Pattern pattern = Pattern.compile(score_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(1) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析上映日期
	private String parsePubDate(String content){
		String result = "P:";
		String score_regax = "<span property=\"v:initialReleaseDate\" content=\"(.*?)\">(.*?)</span>";
		Pattern pattern = Pattern.compile(score_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(1) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
	
	//解析摘要
	private String parseAbstract(String content){
		String tempStr = "";
		Pattern p;
		Matcher m;
		if(!StringUtils.isEmpty(content)){
			p = Pattern.compile("\\s*|\t|\r|\n");
	        m = p.matcher(content);
	        tempStr = m.replaceAll("");
		}
		if(tempStr!=null && !"".equals(tempStr)){
			tempStr = tempStr.replaceAll("<br/>", "").replaceAll("<spanproperty=\"v:summary\">", "");
			return "SUB:"+tempStr;
		}
		return null;
	}
	
	//解析回复总数
	private String parseRelyCount(String content){
		String result = "RES:";
		String score_regax = "<a href=\"(.*?)\"(.*?)>(.*?)</a>";
		Pattern pattern = Pattern.compile(score_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(3) + "、";
		}
		if(result!=null && !"".equals(result)){
			String count = result.substring(0, result.length()-1);
			return count.replace("更多短评", "").replace("条", "");
		}
		return null;
	}
	
	//解析海报
	private String parseImage(String content){
		String result = "IMG:";
		String score_regax = "<img src=\"(.*?)\" title=\"点击看更多海报\" alt=\"(.*?)\" rel=\"v:image\" />";
		Pattern pattern = Pattern.compile(score_regax);
		Matcher matcher = pattern.matcher(content);
		while(matcher.find()) {
			result= result + matcher.group(1) + "、";
		}
		if(result!=null && !"".equals(result)){
			return result.substring(0, result.length()-1);
		}
		return null;
	}
}
