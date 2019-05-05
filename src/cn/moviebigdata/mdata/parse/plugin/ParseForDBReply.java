package cn.moviebigdata.mdata.parse.plugin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYReplyData;
import cn.hadoop.mdata.exception.NoneFetchItemException;

/**
 * ����ظ����ݽ�����
 * @author Administrator
 *
 */
public class ParseForDBReply extends ParseHtml<DBDYReplyData>{
	
	private int nextpage = 0;
	
	@Override
	public DBDYReplyData parse(String content) {
		return null;
	}

	@Override
	public boolean hasNextPage(int curpage) {
		if(nextpage>curpage){
			return true;
		}
		return false;
	}
	
	@Override
	public int nextPage() {
		return nextpage;
	}
	
	@Override
	public List<DBDYReplyData> parsetoList(String content,String _key) throws NoneFetchItemException {
		List<DBDYReplyData> _items = new ArrayList<DBDYReplyData>();
		DBDYReplyData _item;
		Document doc = Jsoup.parse(content);
		//��ȡ������ʼ�ڵ�
		Element start_node = doc.getElementById("comments");
		//�ж������Ƿ����
		Elements hascomments = start_node.getElementsByClass("comment");
		if(hascomments.size()>0){
			Elements comments = start_node.getElementsByClass("comment-item");
			Iterator<Element> it = comments.iterator();
			while(it.hasNext()){
				_item = new DBDYReplyData();
				_item.setFetch_level(Constant.FETCH_LEVEL_REPLY);
				_item.setFetch_status(Constant.FETCH_DONE);
				_item.setFetch_msg(Constant.FETCH_MSG_SUCCESS);
				_item.setTimestamp(System.currentTimeMillis()+"");
				_item.setMid(_key);
				Element child = it.next();
				String comment_id = child.attr("data-cid");
				_item.setReplyID(comment_id);
				//ֱ�Ӹ���P��ǩ��ȡ����
				Elements p_elements = child.getElementsByTag("p");
				Element content_e = p_elements.first();
				int childsize = content_e.childNodeSize();
				if(childsize>1){//�ж����ݵ���Ϣ��Դ
					Node termital= content_e.childNode(1);
					_item.setDevice(termital.attr("alt"));
				}else{
					_item.setDevice("PC");
				}
				Node content_node= content_e.childNode(0);
				_item.setContent(content_node.outerHtml());
				
				//��ȡ��һ���ӽڵ㣬���汣�����û���ID
				Element user_node = child.child(0).child(0);
				String username = user_node.attr("title");
				String userlink = user_node.attr("href");
				_item.setUsername(username);
				_item.setUserurl(userlink);
				_item.setFromuser(userlink);
				
				//��ȡ���۵�����
				Element c_node = child.child(1);
				
				//���õ���������
				Elements yycount = c_node.getElementsByClass("comment-vote");
				Elements yy_element = yycount.get(0).getElementsByClass("votes");
				if(yy_element.size()>0){
					String count =yy_element.get(0).childNode(0).outerHtml();
					_item.setReply_valid(count);
				}
				
				
				//��ȡ�û�������comment-info
				Elements rate_es = c_node.getElementsByClass("comment-info");
				
				//�鿴�ӽڵ������е�SPAN��ǩ
				Elements spans = rate_es.get(0).getElementsByTag("span");
				Iterator<Element> it_span = spans.iterator();
				while(it_span.hasNext()){
					Element span_node = it_span.next();
					if(span_node.hasClass("rating")){
						String rateing =  span_node.attr("class");
						rateing = rateing.replaceAll("\t\r\n", "").replaceAll("allstar", "").replaceAll("rating", "");
						_item.setRating(rateing);
					}else if(span_node.hasClass("comment-info")){
						
					}else{
						_item.setReply_time(span_node.childNode(0).outerHtml());
					}
				}
				_items.add(_item);
			}
			
			//�ж�ʱ������һҳ
			Element nextPage = doc.getElementById("paginator");
			Elements nodes = nextPage.getElementsByTag("a");
			Iterator<Element> a_its = nodes.iterator();
			while(a_its.hasNext()){
				Element next =a_its.next();
				if(next.hasClass("next")){
					String nextparams = next.attr("href");
					if(nextparams!=null && !"".equals(nextparams)){
						nextparams = nextparams.substring(1, nextparams.length());
						String[] tempaggs = nextparams.split("&");
						nextpage = Integer.parseInt((tempaggs[0].split("="))[1]);
					}
					break;
				}
			}
		}else{//���۲�����
			throw new NoneFetchItemException();
		}
		
		return _items;
	}
}
