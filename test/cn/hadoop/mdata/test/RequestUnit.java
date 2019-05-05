package cn.hadoop.mdata.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Test;

import cn.hadoop.mdata.entity.DBDYData;
import cn.hadoop.mdata.entity.DBUserItem;
import cn.moviebigdata.mdata.parse.DBDYParseone;
import cn.moviebigdata.mdata.parse.plugin.ParseForDBUser;
import cn.moviebigdata.mdata.parse.plugin.ParseHtml;
import cn.moviebigdata.mdata.parse.plugin.UnicodeFormat;
import cn.moviebigdata.mdata.request.DBINFORequest;
import cn.moviebigdata.mdata.request.DBListRequest;
import cn.moviebigdata.mdata.request.Request;
import cn.moviebigdata.mdata.request.RequestFactory;
import cn.moviebigdata.mdata.request.client.HttpBaseGetRequest;
import cn.moviebigdata.mdata.request.client.HttpGetRequest;
import cn.moviebigdata.mdata.request.client.HttpPostRequest;
import cn.moviebigdata.mdata.request.client.RequestClient;
import cn.moviebigdata.mdata.response.ClientResponse;

public class RequestUnit {
	
	
	private Request request;
	
	public void test(){
		request = RequestFactory.createRequest(
				DBINFORequest.class, "http://movie.douban.com/subject/1929463/?from=tag_all");
		String result = request.request();
		System.out.println(result);
	}
	
	public void test01() throws Exception{
		request = RequestFactory.createRequest(
				DBListRequest.class, "http://www.douban.com/tag/2012/movie");
		String result = request.request();
		List<DBDYData> items = DBDYParseone.getResult(result);
		System.out.println(result);
	}
	
	@Test
	public void test02() throws Exception{
		RequestClient request = new HttpBaseGetRequest();
		ClientResponse reponse = request.request("http://movie.douban.com/subject/10046172/comments?start=0&limit=20&sort=new_score" ,null);
		System.out.println(reponse.getCode());
		System.out.println(reponse.getProtocol());
		System.out.println(reponse.getContent());
		if(reponse!=null && "200".equals(reponse.getCode())){
//			ParseHtml<DBDYReplyData> parse = new ParseForDBReply();
//			try {
//				parse.parsetoList(reponse.getContent(),"10047547");
//			} catch (NoneFetchItemException e) {
//				System.out.println("抓取数据不存在");
//			}
//			int nextpage = parse.nextPage();
//			System.out.println("NEXT_PAGE:\t" + nextpage);
		}
	}
	
	
	public void testdbuser() throws Exception{
		RequestClient request = new HttpGetRequest();
		ClientResponse reponse = request.request("http://www.douban.com/people/54907599/" ,null);
		System.out.println(reponse.getCode());
		System.out.println(reponse.getProtocol());
		if(reponse!=null && "200".equals(reponse.getCode())){
			ParseHtml<DBUserItem> parse = new ParseForDBUser();
//			DBUserItem _item = parse.parse(reponse.getContent());
			System.out.println(reponse.getContent());
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	public void test03() throws Exception{
		RequestClient request = new HttpPostRequest();
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("country", "0")); 
		params.add(new BasicNameValuePair("page", "39")); 
		params.add(new BasicNameValuePair("type", "0")); 
		params.add(new BasicNameValuePair("year", "2015")); 
		ClientResponse reponse = request.request("http://movie.weibo.com/movie/webajax/category" ,params);
		if(reponse!=null){
			System.out.println(reponse.getCode());
			System.out.println(reponse.getProtocol());
			System.out.println(reponse.getContent());
			System.out.println(UnicodeFormat.decodeUnicode(reponse.getContent()));
		}
	}
	
}
