package cn.moviebigdata.mdata.parse;

import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.junit.Test;

import cn.hadoop.mdata.entity.model.SinaHtmlModel;
import com.alibaba.fastjson.JSON;

public class SinaUserParse {
	
	
	
	public void parse(String content){
		if(!StringUtils.isEmpty(content)){
//			content.replaceAll("<script>FM.view(", "").replaceAll(")</script>", "");
//			SinaHtmlModel model = JSON.parseObject(content, SinaHtmlModel.class);
			Document doc = Jsoup.parseBodyFragment(content);
			Element body = doc.body();
			Elements interests = body.getElementsByAttributeValueMatching("class", "tag S_txt1");
			ListIterator<Element>  iterator = interests.listIterator();
			while(iterator.hasNext()){//输出兴趣
				Element child = iterator.next();
				List<Node> childs = child.childNodes();
				String text = childs.get(0).childNode(0).outerHtml();
				System.out.println(text);
			}
			
			Elements location = body.getElementsByAttributeValueMatching("class", "item_text W_fl");
			while(iterator.hasNext()){//输出兴趣
				Element child = iterator.next();
				List<Node> childs = child.childNodes();
				String text = childs.get(0).childNode(0).outerHtml();
				System.out.println(text);
			}
		}
	}
	
	@Test
	public void test(){
		String content = "<div class=\"WB_cardwrap S_bg2\" fixed-inbox=\"true\">    <!-- v6 card 通用标题 --><div class=\"PCD_person_info\"><div class=\"verify_area W_tog_hover S_line2\"><p class=\"verify clearfix\"><span class=\"icon_bed W_fl\"><a target=\"_blank\" href=\"http://club.weibo.com/intro?from=profile&wvr=6&loc=darenicon\" class=\"W_icon icon_verify_club\"></a></span><span class=\"icon_group S_line1 W_fl\"><a class=\"W_icon_level icon_level_c3\"  title=\"微博等级17\" href=\"http://level.account.weibo.com/level/levelexplain?from=profile2\" target=\"_black\"><span>Lv.17</span></a>&nbsp;</span></p> <p class=\"info\"><span>中级达人</span>&nbsp;&nbsp;&nbsp;<span class=\"S_txt2\">积分：<em class=\"S_txt1\">6589.8</em></span>&nbsp;&nbsp;&nbsp;<span class=\"S_txt2\"><br>兴趣：<em class=\"tag S_txt1\"><a href=\"http://club.weibo.com/list?fun2048=2048&wvr=6&loc=darenint&from=profile\" target=\"_blank\">音乐</a></em><em class=\"tag S_txt1\"><a href=\"http://club.weibo.com/list?fun32=32&wvr=6&loc=darenint&from=profile\" target=\"_blank\">旅游</a></em><em class=\"tag S_txt1\"><a href=\"http://club.weibo.com/list?fun1=1&wvr=6&loc=darenint&from=profile\" target=\"_blank\">美食</a></em></span></p></div><div class=\"WB_innerwrap\"><div class=\"m_wrap\"><div class=\"detail\"><ul class=\"ul_detail\">        <li class=\"item S_line2 clearfix\">        <span class=\"item_ico W_fl\"><em class=\"W_ficon ficon_cd_place S_ficon\">2</em></span>        <span class=\"item_text W_fl\">                湖南 郴州        </span></li>    <li class=\"item S_line2 clearfix\">        <span class=\"item_ico W_fl\"><em class=\"W_ficon ficon_constellation S_ficon\">ö</em></span>        <span class=\"item_text W_fl\">                天蝎座        </span></li>    <li class=\"item S_line2 clearfix\">        <span class=\"item_ico W_fl\"><em class=\"W_ficon ficon_pinfo S_ficon\">Ü</em></span>        <span class=\"item_text W_fl\">            简介：            Boss你永远在我心里 ❤这辈子唯一的偶像        </span></li>    <li class=\"item S_line2 clearfix\">        <span class=\"item_ico W_fl\"><em class=\"W_ficon ficon_link S_ficon\">5</em></span>        <span class=\"item_text W_fl\">            博客地址：            <a target=\"_blank\" href=\"http://weibo.com/u/2829371250\">http://weibo.com/u/2829371250</a>        </span></li>    <li class=\"item S_line2 clearfix\">        <span class=\"item_ico W_fl\"><em class=\"W_ficon ficon_cd_coupon S_ficon\">T</em></span>        <span class=\"item_text W_fl\">        <span class=\"S_txt2\">标签</span>                        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=%E7%88%B1%E5%B9%BB%E6%83%B3&from=profile&wvr=6\">爱幻想</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=%E5%A4%A9%E8%9D%8E%E5%BA%A7&from=profile&wvr=6\">天蝎座</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=shopping&from=profile&wvr=6\">shopping</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=%E7%88%B1%E6%97%85%E8%A1%8C&from=profile&wvr=6\">爱旅行</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=%E7%88%B1%E8%87%AA%E7%94%B1&from=profile&wvr=6\">爱自由</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=%E7%BE%8E%E9%A3%9F%E7%88%B1%E5%A5%BD%E8%80%85&from=profile&wvr=6\">美食爱好者</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=k%E6%AD%8C&from=profile&wvr=6\">k歌</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=VIFC&from=profile&wvr=6\">VIFC</a>        <a target=\"_blank\" href=\"http://s.weibo.com/user/&tag=%E5%90%B3%E5%BB%BA%E8%B1%AA&from=profile&wvr=6\">吳建豪</a>            </span></li></ul></div></div></div><a class=\"WB_cardmore S_txt1 S_line1 clearfix\"     href=\"javascript:;\" action-type=\"login\"><span class=\"more_txt\">查看更多&nbsp;<em class=\"W_ficon ficon_arrow_right S_ficon\">a</em></span></a></div></div>";
		parse(content);
	}
	
}
