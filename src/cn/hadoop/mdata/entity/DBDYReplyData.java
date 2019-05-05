package cn.hadoop.mdata.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 豆瓣电影回复系统的Model
 * @author Administrator
 *
 */
public class DBDYReplyData implements Writable,Cloneable{
	
	public DBDYReplyData() {
	}

	private byte version = 0x01; // 记录当前版本
	
	private byte fetch_status; //记录当前状态,初始状态为0
	
	private byte fetch_level; //抓取的阶段状态
	
	private String fetch_msg; //记录抓取成功或者错误状态信息
	
	private String timestamp; //抓取的时间戳
	
	/**
	 * 回复ID
	 */
	private String replyID = "-";
	
	/**
	 * 电影的key
	 */
	private String mid = "-";
	
	/**
	 * 评论用户的id
	 */
	private String fromuser = "-";
	
	
	/**
	 * 回复的时间
	 */
	private String reply_time = "-";
	
	/**
	 * 评论者的姓名
	 */
	private String username = "-";
	
	
	/**
	 * 评论内容的有用性
	 */
	private String reply_valid = "-";
	
	/**
	 * 评论内容的星级
	 */
	private String rating = "-";
	
	/**
	 * 评论的内容
	 */
	private String content = "-";
	
	/**
	 * 评论者的用户主页
	 */
	private String userurl = "-";
	
	
	/**
	 * 评论者的终端来源
	 */
	private String device = "-";
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		version = in.readByte();
		fetch_status = in.readByte();
		fetch_level = in.readByte();
		fetch_msg = in.readUTF();
		replyID = in.readUTF();
		mid = in.readUTF();
		fromuser = in.readUTF();
		reply_time = in.readUTF();
		username = in.readUTF();
		reply_valid = in.readUTF();
		rating = in.readUTF();
		content = in.readUTF();
		userurl = in.readUTF();
		device = in.readUTF();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(version);
		out.writeByte(fetch_status);
		out.writeByte(fetch_level);
		out.writeUTF(fetch_msg);
		out.writeUTF(replyID);
		out.writeUTF(mid);
		out.writeUTF(fromuser);
		out.writeUTF(reply_time);
		out.writeUTF(username);
		out.writeUTF(reply_valid);
		out.writeUTF(rating);
		out.writeUTF(content);
		out.writeUTF(userurl);
		out.writeUTF(device);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("version:" + version +"\n")
		.append("status:" + fetch_status +"\n")
		.append("fetch_level:" + fetch_level +"\n")
		.append("fetch_msg:" + fetch_msg +"\n")
		.append("replyID:" + replyID +"\n")
		.append("mid:" + mid +"\n")
		.append("fromuser:" + fromuser +"\n")
		.append("reply_time:" + reply_time +"\n")
		.append("username:" + username +"\n")
		.append("reply_valid:" + reply_valid +"\n")
		.append("rating:" + rating +"\n")
		.append("content:" + content +"\n")
		.append("userurl:" + userurl +"\n")
		.append("device:" + device +"\n");
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
	 * @return the replyID
	 */
	public String getReplyID() {
		return replyID;
	}

	/**
	 * @param replyID the replyID to set
	 */
	public void setReplyID(String replyID) {
		this.replyID = replyID;
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
		this.mid = mid;
	}

	/**
	 * @return the fromuser
	 */
	public String getFromuser() {
		return fromuser;
	}

	/**
	 * @param fromuser the fromuser to set
	 */
	public void setFromuser(String fromuser) {
		this.fromuser = fromuser;
	}

	/**
	 * @return the reply_time
	 */
	public String getReply_time() {
		return reply_time;
	}

	/**
	 * @param reply_time the reply_time to set
	 */
	public void setReply_time(String reply_time) {
		this.reply_time = reply_time;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the reply_valid
	 */
	public String getReply_valid() {
		return reply_valid;
	}

	/**
	 * @param reply_valid the reply_valid to set
	 */
	public void setReply_valid(String reply_valid) {
		this.reply_valid = reply_valid;
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
	 * @return the content
	 */
	public String getContent() {
		return content;
	}

	/**
	 * @param content the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}

	/**
	 * @return the userurl
	 */
	public String getUserurl() {
		return userurl;
	}

	/**
	 * @param userurl the userurl to set
	 */
	public void setUserurl(String userurl) {
		this.userurl = userurl;
	}

	/**
	 * @return the device
	 */
	public String getDevice() {
		return device;
	}

	/**
	 * @param device the device to set
	 */
	public void setDevice(String device) {
		this.device = device;
	}
	
}
