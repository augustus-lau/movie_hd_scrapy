package cn.moviebigdata.mdata.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;

/**
 * 这里写正则对数据进行过滤
 * 
 * @author Administrator
 * 
 */
public class DBDYParseone {

	private static final String title_regex = "<ahref=\"(.*?)\"target=\"_blank\">(.*?)</a>";
	private static final String image_clean = "<imgsrc=\"(.*?)\"/>";

	public static List<DBDYData> getResult(String content) throws Exception{
		List<DBDYData> result = new ArrayList<DBDYData>();

		StringBuffer titleBuffer = new StringBuffer();

		DBDYData item = null;
		if (content.startsWith("SUCCESS:")) {
			// 如果结果返回正常

			// 去掉空格，换行，TAB
			Pattern p = Pattern.compile("\\s*|\t|\r|\n");
			Matcher m = p.matcher(content);
			String tempStr = m.replaceAll("");

			// 先提取电影缩略图
			p = Pattern.compile(image_clean);
			m = p.matcher(tempStr);
			tempStr = m.replaceAll("");
			
			// 提取标题
			p = Pattern.compile(title_regex);
			m = p.matcher(tempStr);
			if (m.groupCount() > 0) {
				while (m.find()) {
					if(m.group(2)!=null && !"".equals(m.group(2))){
						titleBuffer.append(m.group(1) + "~"+m.group(2)+"\t");
					}
				}
			}
			
			String titlesrt = titleBuffer.toString();
			String[] titles = titlesrt.substring(0, titlesrt.length()-1).split("\t");
			
			for(int i=0;i<titles.length;i++){
				String[] splits = titles[i].split("~");
				item = new DBDYData();
				item.setVersion(Constant.FETCH_VERSION);
				item.setFetch_status(Constant.FETCH_DONE);
				item.setFetch_msg("_SUCCESS");
				item.setName(splits[1]);
				String url = splits[0];
				String mid = "";
				if(url!=null && !"".equals(url)){
					mid = url.substring(url.indexOf("subject/")+8, url.indexOf("/?from=tag_all"));
					item.setMid(mid);
					item.setMovieurl("http://movie.douban.com/subject/"+mid+"/?from=tag_all");
				}else{
					item.setMovieurl(splits[0]);
				}
				result.add(item);
			}
			return result;

		} else if (content.startsWith("ERRORCODE:")) {
			item = new DBDYData();
			// 如果爆粗，这里对mid设定一个特殊的数字-1;
			String code = content.substring(10, content.length() - 12);
			item.setVersion(Constant.FETCH_VERSION);
			item.setFetch_status(Constant.FETCH_NETWORK_ERROR);
			item.setFetch_msg(code);
			result.add(item);
			return result;
		} else if (content.startsWith("ERRORMSG:")) {
			item = new DBDYData();
			String msg = content.substring(9, content.length() - 11);
			item.setVersion(Constant.FETCH_VERSION);
			item.setFetch_status(Constant.FETCH_EXCEPTION);
			item.setFetch_msg(msg);
			result.add(item);
			return result;
		} else if (content.startsWith("HALT:")) {
			item = new DBDYData();
			item.setVersion(Constant.FETCH_VERSION);
			item.setFetch_status(Constant.FETCH_END_SIGNAL);
			result.add(item);
			return result;
		} else {
			return null;
		}

	}

}
