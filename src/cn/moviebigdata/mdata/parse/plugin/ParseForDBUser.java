package cn.moviebigdata.mdata.parse.plugin;

import java.util.List;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.hadoop.mdata.entity.DBUserItem;

/**
 * 豆瓣用户页面的解析器，解析住址和心情
 * @author Administrator
 *
 */
public class ParseForDBUser extends ParseHtml<DBUserItem>{
	
	
	@Override
	public DBUserItem parse(String content) {
		DBUserItem _item = new DBUserItem();
		Document doc = Jsoup.parse(content);
		//获取该人的相关介绍
		Element intro_node = doc.getElementById("intro_display");
		if(intro_node.childNodeSize()>0){
			_item.setDispalyinfo(intro_node.child(0).outerHtml());
			System.out.println(intro_node.html());
		}
		
		Elements userinfos = doc.getElementsByClass("user-info");
		if(userinfos.size()>0){
			Elements as = userinfos.get(0).getElementsByTag("a");
			if(as.size()>0){
				System.out.println(as.get(0).html());
			}
		}
		return _item;
	}

	@Override
	public List<DBUserItem> parsetoList(String content,String _key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNextPage(int curpage) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int nextPage() {
		// TODO Auto-generated method stub
		return 0;
	}

}
