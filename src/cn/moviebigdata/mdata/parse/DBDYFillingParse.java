package cn.moviebigdata.mdata.parse;

import org.apache.commons.lang.StringUtils;

import cn.hadoop.mdata.common.Constant;
import cn.hadoop.mdata.entity.DBDYData;

public class DBDYFillingParse {
	
	/**
	 * [SUCCESS:]
	 * IMG:http://img3.douban.com/view/movie_poster_cover/spst/public/p1784592701.jpg
	 * #D:�
	 * #W:����ض������������
	 * #A:������ɳ�ꡢ���ơ�˹�������������ɺ�����������������Լ�������������ա������򡢰���ʲ��̹�������ؽܡ���������
	 * #G:���顢��á�ð��
	 * #U: ���� / ̨�� / Ӣ�� / ���ô�
	 * #Y: Ӣ�� / ̩�׶��� / ���� / ���� / ӡ���� / ������ͨ��
	 * #P:2012-11-22(�й���½)��2012-09-28(ŦԼ��Ӱ��)��2012-11-21(����)
	 * #T:127����
	 * #I:tt0454876
	 * #S:9.0
	 * #RES:170894
	 * #SUB:�������¿�ʼ������¦��Ҳ����������¦��һ������Ѱ��е����ң����ơ�˹��RafeSpall�Σ�������֪�ɡ������������������ɺ�IrrfanKhan�Σ��Ĵ�����¡��ɵĸ��ף������ա�������AdilHussain�Σ�����һ�Ҷ���԰�������������������������ɣ�������ɳ��SurajSharma�Σ����������˵ı�������һ�׿���������17����һ�꣬���ĸ�ĸ�����ټ�������ô���׷����õ��������Ҳ�����뿪���ĳ������ˡ���ǰ�����ô�Ĵ��ϣ���������һλ���̳��Եķ�����ʦ����������������ԼG��rardDepardieu�Σ���������ҹ��ãã���У�ԭ�����ɸе��̼��ޱȵı�����һ˲��ͳ������ɻ����Ĵ����ѡ���ȴ�漣��ػ������������ž�������̫ƽ����Ư����������һ�����������벻����ͬ�顪�����¡����ˣ�һֻ�ϼ����ϻ��������ð���ó̾�������...
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
						_item.setWriters(writers.split("��"));
					}else if(split.startsWith("A:") && split.length()>2){
						String actors = split.split(":")[1];
						_item.setActors(actors.split("��"));
					}else if(split.startsWith("G:") && split.length()>2){
						String geres = split.split(":")[1];
						_item.setSort(geres.split("��"));
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
