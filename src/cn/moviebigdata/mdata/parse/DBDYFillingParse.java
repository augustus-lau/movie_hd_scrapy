package cn.moviebigdata.mdata.parse;

import org.apache.commons.lang.StringUtils;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;

public class DBDYFillingParse {
	
	/**
	 * [SUCCESS:]
	 * IMG:http://img3.douban.com/view/movie_poster_cover/spst/public/p1784592701.jpg
	 * #D:李安
	 * #W:扬·马特尔、大卫·麦基
	 * #A:苏拉·沙玛、拉菲·斯波、伊尔凡·可汗、杰拉尔·德帕迪约、塔布、阿迪勒·侯赛因、阿尤什·坦东、王柏杰、俊·奈托
	 * #G:剧情、奇幻、冒险
	 * #U: 美国 / 台湾 / 英国 / 加拿大
	 * #Y: 英语 / 泰米尔语 / 法语 / 日语 / 印地语 / 汉语普通话
	 * #P:2012-11-22(中国大陆)、2012-09-28(纽约电影节)、2012-11-21(美国)
	 * #T:127分钟
	 * #I:tt0454876
	 * #S:9.0
	 * #RES:170894
	 * #SUB:　　故事开始于蒙特娄，也结束于蒙特娄。一名在找寻灵感的作家（拉菲·斯波RafeSpall饰）无意间得知派·帕帖尔（伊尔凡·可汗IrrfanKhan饰）的传奇故事。派的父亲（阿迪勒·侯赛因AdilHussain饰）开了一家动物园。因这样特殊的生活环境，少年派（苏拉·沙玛SurajSharma饰）对信仰与人的本性自有一套看法。在派17岁那一年，他的父母决定举家移民加拿大以追求更好的生活，而他也必须离开他的初恋情人。在前往加拿大的船上，他们遇见一位残忍成性的法国厨师（杰拉尔·德帕迪约GérardDepardieu饰）。当天深夜在茫茫大海中，原本令派感到刺激无比的暴风雨一瞬间就成了吞噬货船的大灾难。派却奇迹般地活了下来，搭着救生船在太平洋上漂流，而且有一名最令人意想不到的同伴——理查德·帕克，一只孟加拉老虎。神奇的冒险旅程就这样意...
	 * @param _item
	 * @param content
	 * @throws Exception
	 */
	public void parse(DBDYData _item , String content) throws Exception{
		if(!StringUtils.isEmpty(content) && !"".equals(content)){
			if(content.startsWith("SUCCESS=")){
				content = content.trim();
				_item.setFetch_status(Constant.FETCH_DONE);
				_item.setFetch_level(Constant.FETCH_LEVEL_FILLING);
				_item.setFetch_msg(Constant.FETCH_MSG_SUCCESS);
				String[] content_splits = content.replaceAll("SUCCESS=", "").split("#");
				for(String split : content_splits){
					if(split.startsWith("IMG:") && split.length()>4){
						_item.setImage(split.substring(5, split.length()));
					}else if(split.startsWith("D:") && split.length()>2){
						_item.setDirector(split.split(":")[1]);
					}else if(split.startsWith("W:") && split.length()>2){
						String writers = split.split(":")[1];
						_item.setWriters(writers.split("、"));
					}else if(split.startsWith("A:") && split.length()>2){
						String actors = split.split(":")[1];
						_item.setActors(actors.split("、"));
					}else if(split.startsWith("G:") && split.length()>2){
						String geres = split.split(":")[1];
						_item.setSort(geres.split("、"));
					}else if(split.startsWith("U:") && split.length()>2){
						_item.setCountry(split.split(":")[1]);
					}else if(split.startsWith("Y:") && split.length()>2){
//						_item.setCountry(split.split(":")[1]);
					}else if(split.startsWith("P:") && split.length()>2){
						String pub = new String(split.split(":")[1].getBytes(),0,split.split(":")[1].length(),"UTF-8");
						_item.setRelease(pub);
					}else if(split.startsWith("T:") && split.length()>2){
						String time = new String(split.split(":")[1].getBytes(),0,split.split(":")[1].length(),"UTF-8");
						_item.setRtime(time);
					}else if(split.startsWith("I:") && split.length()>2){
						_item.setImdbid(split.split(":")[1]);
					}else if(split.startsWith("S:") && split.length()>2){
						_item.setRating(split.split(":")[1]);
					}else if(split.startsWith("RES:") && split.length()>4){
						try {
							int count = Integer.parseInt(split.split(":")[1]);
							_item.setReplycount(count);
						} catch (Exception e) {
							_item.setReplycount(0);
						}
					}else if(split.startsWith("SUB:") && split.length()>4){
						_item.setDesc(split.substring(4, split.length()));
					}
					String mid = _item.getMid();
					_item.setCommentsurl("http://movie.douban.com/subject/"+mid+"/comments");
					_item.setDiscussionurl("http://movie.douban.com/subject/"+mid+"/reviews");
					_item.setReplysid(mid);
				}
			}else if(content.startsWith("ERRORMSG:")){
				_item.setFetch_status(Constant.FETCH_EXCEPTION);
				_item.setFetch_level(Constant.FETCH_LEVEL_FILLING);
				_item.setFetch_msg(Constant.FETCH_MSG_ERROR);
			}
		}else{
			_item.setFetch_status(Constant.FETCH_NETWORK_ERROR);
			_item.setFetch_level(Constant.FETCH_LEVEL_FILLING);
			_item.setFetch_msg(Constant.FETCH_MSG_NULL);
		}
	}
}
