package cn.hadoop.mdata.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 豆瓣电影的电影Model
 * @author Administrator
 * fetch_status=0表示正常状态
 * fetch_status=1表示程序报错状态
 * fetch_status=2表示网络异常状态（网络联结问题）
 */
public class DBDYData implements WritableComparable<MData>, Cloneable{
	
	public DBDYData() {
		this.fetch_status = 0x00;
		this.fetch_level = 0x00;
		this.fetch_msg = "-";
		this.timestamp = "-";
		this.frompage = "-";
		this.mid = "-";
		this.name = "-";
		this.othername = "-";
		this.image = "-";
		this.director = "-";
		this.replysid = "-";
		this.replycount = 0;
		this.release = "0";
		this.country = "-";
		this.desc = "-";
		this.imdbid = "-";
		this.movieurl = "-";
		this.discussionurl = "-";
		this.commentsurl = "-";
		this.rtime = "0";
		this.rating = "0";
		
	}
	
	private byte version = 0x01; //记录当前版本
	
	private byte fetch_status; //记录当前状态,初始状态为0
	
	private byte fetch_level; //抓取的阶段状态
	
	private String fetch_msg; //记录抓取成功或者错误状态信息
	
	
	private String timestamp; //抓取的时间戳
	
	private String frompage;  //当前记录所在的页面
	
	/**
	 * key
	 */
	private String mid;
	
	
	/**
	 * 电影名称
	 */
	private String name;
	
	
	/**
	 * 电影别名
	 */
	private String othername;
	
	/**
	 * 电影缩略图
	 */
	private String image;
	
	/**
	 * 演员
	 */
	private String[] actors;
	private TextArrayWritable actors_text;
	
	/**
	 * 电影的分类
	 */
	private String[] sort;
	private TextArrayWritable sorts_text;
	
	/**
	 * 编剧
	 */
	private String[] writers;
	private TextArrayWritable writers_text;
	
	/**
	 * 导演
	 */
	private String director;
	
	/**
	 * 该电影的回复ID,用户关联电影回复信息
	 */
	private String replysid;
	
	/**
	 * 回复总数
	 */
	private int replycount;
	
	/**
	 * 影片的上映日期
	 */
	private String release;
	
	/**
	 * 来源国家
	 */
	private String country;
	
	/**
	 * 电影描述
	 */
	private String desc;
	
	/**
	 * 电影时长
	 */
	private String rtime;
	
	
	/**
	 * 电影评分
	 */
	private String rating;
	
	/**
	 * imdb的关联ID
	 */
	private String imdbid;
	
	/**
	 * 该电影的主页
	 */
	private String movieurl;
	
	/**
	 * 该电影的讨论区地址
	 */
	private String discussionurl;
	
	/**
	 * 该电影的影评地址
	 */
	private String commentsurl;
	
	
	public class TextArrayWritable extends ArrayWritable {

		public TextArrayWritable(Class<? extends Writable> valueClass) {
			super(valueClass);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		version = in.readByte();
		fetch_status = in.readByte();
		fetch_level = in.readByte();
		fetch_msg = in.readUTF();
		timestamp = in.readUTF();
		frompage = in.readUTF();
		mid = in.readUTF();
		name = in.readUTF();
		othername = in.readUTF();
		image = in.readUTF();
		if(in.readBoolean()){
			actors_text = new TextArrayWritable(Text.class);
			actors_text.readFields(in);
		}
		if(in.readBoolean()){
			sorts_text = new TextArrayWritable(Text.class);
			sorts_text.readFields(in);
		}
		if(in.readBoolean()){
			writers_text = new TextArrayWritable(Text.class);
			writers_text.readFields(in);
		}
		director = in.readUTF();
		replysid = in.readUTF();
		replycount = in.readInt();
		release = in.readUTF();
		country = in.readUTF();
		desc = in.readUTF();
		rtime = in.readUTF();
		rating = in.readUTF();
		imdbid = in.readUTF();
		movieurl = in.readUTF();
		discussionurl = in.readUTF();
		commentsurl = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(version);
		out.write(fetch_status);
		out.write(fetch_level);
		out.writeUTF(fetch_msg);
		out.writeUTF(timestamp);
		out.writeUTF(frompage);
		out.writeUTF(mid);
		out.writeUTF(name);
		out.writeUTF(othername);
		out.writeUTF(image);
		if(actors_text!=null){
			out.writeBoolean(true);//表示演员不为空
			this.actors_text.write(out);
		}else{
			out.writeBoolean(false);//表示演员为空
		}
		if(sorts_text!=null){
			out.writeBoolean(true);//表示电影分类不为空
			this.sorts_text.write(out);
		}else{
			out.writeBoolean(false);//表示电影分类为空
		}
		if(writers_text!=null){
			out.writeBoolean(true);//表示编剧不为空
			this.writers_text.write(out);
		}else{
			out.writeBoolean(false);//表示编剧为空
		}
		out.writeUTF(director);
		out.writeUTF(replysid);
		out.writeInt(replycount);
		out.writeUTF(release);
		out.writeUTF(country);
		out.writeUTF(desc);
		out.writeUTF(rtime);
		out.writeUTF(rating);
		out.writeUTF(imdbid);
		out.writeUTF(movieurl);
		out.writeUTF(discussionurl);
		out.writeUTF(commentsurl);
	}

	@Override
	public int compareTo(MData other) {
		String thatmid = other.getMid();
		String thismid = this.mid;
		return (Integer.parseInt(thatmid) - Integer.parseInt(thismid)) > 0 ? 1
				: -1;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		 buffer.append("VERSION:"+version +"\n")
		.append("FETCH_STATUS:"+fetch_status +"\n")
		.append("FETCH_LEVEL:"+fetch_level +"\n")
		.append("FETCH_MSG:"+fetch_msg+"\n")
		.append("TIMESTAMP:"+timestamp+"\n")
		.append("FROMPAGE:"+frompage+"\n")
		.append("MID:"+mid+"\n")
		.append("NAME:"+name+"\n")
		.append("OTHERNAME:"+othername+"\n")
		.append("IMAGE:"+image+"\n")
		.append("ACTORS:");
		if(actors_text!=null){
			for(String actor : actors_text.toStrings()){
				buffer.append(actor).append("、");
			}
		}
		buffer.append("\n").append("SORTS:");
		if(sorts_text!=null){
			for(String actor : sorts_text.toStrings()){
				buffer.append(actor).append("、");
			}
		}
		buffer.append("\n").append("WRITERS:");
		if(writers_text!=null){
			for(String actor : sorts_text.toStrings()){
				buffer.append(actor).append("、");
			}
		}
		 buffer.append("\n")
		.append("DIRECTOR:" + director +"\n")
		.append("REPLYSID:" + replysid +"\n")
		.append("REPLYCOUNT:" + replycount +"\n")
		.append("RELEASE:" + release +"\n")
		.append("COUNTRY:" + country +"\n")
		.append("DESC:" + desc +"\n")
		.append("RTIME:" + rtime +"\n")
		.append("RATING:" + rating +"\n")
		.append("IMDBID:" + imdbid +"\n")
		.append("MOVIEURL:" + movieurl +"\n")
		.append("DISCUSSIONURL:" + discussionurl +"\n")
		.append("COMMENTSURL:" + commentsurl +"\n");
		return buffer.toString();
		
	}

	/**
	 * @return the version
	 */
	public byte getVersion() {
		return version;
	}

	/**
	 * @param version the version to set
	 */
	public void setVersion(byte version) {
		this.version = version;
	}

	/**
	 * @return the fetch_status
	 */
	public byte getFetch_status() {
		return fetch_status;
	}

	/**
	 * @param fetch_status the fetch_status to set
	 */
	public void setFetch_status(byte fetch_status) {
		this.fetch_status = fetch_status;
	}

	/**
	 * @return the fetch_msg
	 */
	public String getFetch_msg() {
		return fetch_msg;
	}

	/**
	 * @param fetch_msg the fetch_msg to set
	 */
	public void setFetch_msg(String fetch_msg) {
		this.fetch_msg = fetch_msg;
	}

	/**
	 * @return the timestamp
	 */
	public String getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return the mid
	 */
	public String getMid() {
		return mid;
	}

	/**
	 * @param mid the mid to set
	 */
	public void setMid(String mid) {
		if(mid!=null && !"".equals(mid)){
			this.mid = mid;
		}else{
			this.mid = "-";
		}
		
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the othername
	 */
	public String getOthername() {
		return othername;
	}

	/**
	 * @param othername the othername to set
	 */
	public void setOthername(String othername) {
		this.othername = othername;
	}

	/**
	 * @return the actors
	 */
	public String[] getActors() {
		return actors;
	}

	/**
	 * @param actors the actors to set
	 */
	public void setActors(String[] actors) {
		this.actors = actors;
		if(actors!=null && actors.length>0){
			this.actors_text = new TextArrayWritable(actors);
		}
	}

	/**
	 * @return the actors_text
	 */
	public TextArrayWritable getActors_text() {
		return actors_text;
	}

	/**
	 * @param actors_text the actors_text to set
	 */
	public void setActors_text(TextArrayWritable actors_text) {
		this.actors_text = actors_text;
	}

	/**
	 * @return the sort
	 */
	public String[] getSort() {
		return sort;
	}

	/**
	 * @param sort the sort to set
	 */
	public void setSort(String[] sort) {
		this.sort = sort;
		if(sort!=null && sort.length>0){
			this.sorts_text = new TextArrayWritable(sort);
		}
	}

	/**
	 * @return the sorts_text
	 */
	public TextArrayWritable getSorts_text() {
		return sorts_text;
	}

	/**
	 * @param sorts_text the sorts_text to set
	 */
	public void setSorts_text(TextArrayWritable sorts_text) {
		this.sorts_text = sorts_text;
	}

	/**
	 * @return the writers
	 */
	public String[] getWriters() {
		return writers;
	}

	/**
	 * @param writers the writers to set
	 */
	public void setWriters(String[] writers) {
		this.writers = writers;
		if(writers!=null && writers.length>0){
			this.writers_text = new TextArrayWritable(writers);
		}
	}

	/**
	 * @return the writers_text
	 */
	public TextArrayWritable getWriters_text() {
		return writers_text;
	}

	/**
	 * @param writers_text the writers_text to set
	 */
	public void setWriters_text(TextArrayWritable writers_text) {
		this.writers_text = writers_text;
	}

	/**
	 * @return the director
	 */
	public String getDirector() {
		return director;
	}

	/**
	 * @param director the director to set
	 */
	public void setDirector(String director) {
		this.director = director;
	}

	/**
	 * @return the replysid
	 */
	public String getReplysid() {
		return replysid;
	}

	/**
	 * @param replysid the replysid to set
	 */
	public void setReplysid(String replysid) {
		this.replysid = replysid;
	}

	/**
	 * @return the replycount
	 */
	public int getReplycount() {
		return replycount;
	}

	/**
	 * @param replycount the replycount to set
	 */
	public void setReplycount(int replycount) {
		this.replycount = replycount;
	}


	/**
	 * @return the country
	 */
	public String getCountry() {
		return country;
	}

	/**
	 * @param country the country to set
	 */
	public void setCountry(String country) {
		this.country = country;
	}

	/**
	 * @return the desc
	 */
	public String getDesc() {
		return desc;
	}

	/**
	 * @param desc the desc to set
	 */
	public void setDesc(String desc) {
		this.desc = desc;
	}


	public String getRelease() {
		return release;
	}

	public void setRelease(String release) {
		this.release = release;
	}

	public String getRtime() {
		return rtime;
	}

	public void setRtime(String rtime) {
		this.rtime = rtime;
	}

	/**
	 * @return the imdbid
	 */
	public String getImdbid() {
		return imdbid;
	}

	/**
	 * @param imdbid the imdbid to set
	 */
	public void setImdbid(String imdbid) {
		this.imdbid = imdbid;
	}

	/**
	 * @return the movieurl
	 */
	public String getMovieurl() {
		return movieurl;
	}

	/**
	 * @param movieurl the movieurl to set
	 */
	public void setMovieurl(String movieurl) {
		this.movieurl = movieurl;
	}

	/**
	 * @return the discussionurl
	 */
	public String getDiscussionurl() {
		return discussionurl;
	}

	/**
	 * @param discussionurl the discussionurl to set
	 */
	public void setDiscussionurl(String discussionurl) {
		this.discussionurl = discussionurl;
	}

	/**
	 * @return the commentsurl
	 */
	public String getCommentsurl() {
		return commentsurl;
	}

	/**
	 * @param commentsurl the commentsurl to set
	 */
	public void setCommentsurl(String commentsurl) {
		this.commentsurl = commentsurl;
	}

	/**
	 * @return the frompage
	 */
	public String getFrompage() {
		return frompage;
	}

	/**
	 * @param frompage the frompage to set
	 */
	public void setFrompage(String frompage) {
		this.frompage = frompage;
	}

	/**
	 * @return the image
	 */
	public String getImage() {
		return image;
	}

	/**
	 * @param image the image to set
	 */
	public void setImage(String image) {
		this.image = image;
	}

	/**
	 * @return the rating
	 */
	public String getRating() {
		return rating;
	}

	/**
	 * @param rating the rating to set
	 */
	public void setRating(String rating) {
		this.rating = rating;
	}

	/**
	 * @return the fetch_level
	 */
	public byte getFetch_level() {
		return fetch_level;
	}

	/**
	 * @param fetch_level the fetch_level to set
	 */
	public void setFetch_level(byte fetch_level) {
		this.fetch_level = fetch_level;
	}
	
}
