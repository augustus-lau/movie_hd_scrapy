package cn.moviebigdata.mdata.parse.plugin;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSON;

import cn.hadoop.mdata.entity.MovieSinaJson;

public class ParseForSinaMovie extends ParseHtml<MovieSinaJson>{
	
	private static final Log logger = LogFactory.getLog(ParseForSinaMovie.class);
	
	private static final int limit = 21;
	
	private int targetPage = 0;
	
	private int totalPage = 0;
	
	
	@Override
	public MovieSinaJson parse(String content) {
		logger.info("ÕýÔÚ½âÎö£º\t" + content);
		MovieSinaJson data = JSON.parseObject(content, MovieSinaJson.class);
		if(data.getData().getList()!=null && data.getData().getList().size()>0){
			int total = data.getData().getTotal();
			totalPage = total%limit >0 ? (total/limit)+1 : total/limit;
		}
		return data;
	}

	@Override
	public List<MovieSinaJson> parsetoList(String content, String _key) {
		
		return null;
	}

	@Override
	public boolean hasNextPage(int curPage) {
		if(curPage < totalPage){
			targetPage = curPage;
			return true;
		}
		return false;
	}

	@Override
	public int nextPage() {
		return targetPage+1;
	}

}
