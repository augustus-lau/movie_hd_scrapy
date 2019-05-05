package cn.hadoop.mdata.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
public class BTData implements WritableComparable<MData>, Cloneable{
	
	private byte CUR_VERSION = 0x01;
	private byte STATUS = 0x01;
	private String mid , name , todburl , dburl;
	private String director , scriptwriter , pubdate , country , runtime, imdbmid , sorce;
	private String[] actorsarry, genresarry;
	private ArrayWritable actors, genres;
	private String commenturl,reviewsurl;//¶ÌÆÀ£¬Ó°ÆÀ
	
	public BTData() {}
	
	public BTData(String mid,byte status, String name, String todburl, String dburl,
			String director, String scriptwriter, String pubdate,
			String country, String runtime, String imdbmid,
			String[] actorsarry, String[] genresarry,String commenturl,String reviewsurl,String sorce) {
		super();
		this.mid = mid;
		this.STATUS = status;
		if(name!=null && !"".equals(name)){
			this.name = name;
		}else{
			this.name = "-";
		}
		if(todburl!=null && !"".equals(todburl)){
			this.todburl = todburl;
		}else{
			this.todburl = "-";
		}
		if(dburl!=null && !"".equals(dburl)){
			this.dburl = dburl;
		}else{
			this.dburl = "-";
		}
		if(director!=null && !"".equals(director)){
			this.director = director;
		}else{
			this.director = "-";
		}
		if(scriptwriter!=null && !"".equals(scriptwriter)){
			this.scriptwriter = scriptwriter;
		}else{
			this.scriptwriter = "-";
		}
		if(pubdate!=null && !"".equals(pubdate)){
			this.pubdate = pubdate;
		}else{
			this.pubdate = "-";
		}
		if(country!=null && !"".equals(country)){
			this.country = country;
		}else{
			this.country = "-";
		}
		if(runtime!=null && !"".equals(runtime)){
			this.runtime = runtime;
		}else{
			this.runtime = "-";
		}
		if(imdbmid!=null && !"".equals(imdbmid)){
			this.imdbmid = imdbmid;
		}else{
			this.imdbmid = "-";
		}
		if(commenturl!=null && !"".equals(commenturl)){
			this.commenturl = commenturl;
		}else{
			this.commenturl = "-";
		}
		if(reviewsurl!=null && !"".equals(reviewsurl)){
			this.reviewsurl = reviewsurl;
		}else{
			this.reviewsurl = "-";
		}
		if(sorce!=null && !"".equals(sorce)){
			this.sorce = sorce;
		}else{
			this.sorce = "-";
		}
		this.actorsarry = actorsarry;
		this.genresarry = genresarry;
		if(actorsarry!=null && actorsarry.length>0){
			actors = new TextArrayWritable(actorsarry);
		}
		if(genresarry!=null && genresarry.length>0){
			genres = new TextArrayWritable(genresarry);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		CUR_VERSION = in.readByte();
		STATUS = in.readByte();
		mid = in.readUTF();
		name = in.readUTF();
		sorce = in.readUTF();
		director = in.readUTF();
		scriptwriter = in.readUTF();
		pubdate = in.readUTF();
		country = in.readUTF();
		runtime = in.readUTF();
		imdbmid = in.readUTF();
		todburl = in.readUTF();
		dburl = in.readUTF();
		commenturl = in.readUTF();
		reviewsurl = in.readUTF();
		boolean hasActors = in.readBoolean();
		if(hasActors){
			actors = new TextArrayWritable(Text.class);
			actors.readFields(in);
		}
		boolean hasGenres = in.readBoolean();
		if(hasGenres){
			genres = new TextArrayWritable(Text.class);
			genres.readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(CUR_VERSION);
		out.write(STATUS);
		out.writeUTF(mid);
		out.writeUTF(name);
		out.writeUTF(sorce);
		out.writeUTF(director);
		out.writeUTF(scriptwriter);
		out.writeUTF(pubdate);
		out.writeUTF(country);
		out.writeUTF(runtime);
		out.writeUTF(imdbmid);
		out.writeUTF(todburl);
		out.writeUTF(dburl);
		out.writeUTF(commenturl);
		out.writeUTF(reviewsurl);
		if(actors!=null){
			out.writeBoolean(true);
			actors.write(out);
		}else{
			out.writeBoolean(false);
		}
		if(genres!=null){
			out.writeBoolean(true);
			genres.write(out);
		}else{
			out.writeBoolean(false);
		}
		
	}

	@Override
	public int compareTo(MData that) {
		String thatmid = that.getMid();
		String thismid = this.mid;
		return (Integer.parseInt(thatmid) - Integer.parseInt(thismid)) > 0 ? 1
				: -1;
	}
	
	
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	protected Object clone() throws CloneNotSupportedException {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("CUR_VERSION: " + CUR_VERSION +"\n").append("STATUS: " + STATUS +"\n")
		.append("mid: " + mid +"\n").append("name: " + name +"\n").append("sorce: " + sorce +"\n")
		.append("director: " + director +"\n").append("scriptwriter: " + scriptwriter +"\n")
		.append("pubdate : " + pubdate + "\n")
		.append("country: " + country + "\n")
		.append("runtime: " + runtime + "\n")
		.append("imdbmid: " + imdbmid + "\n")
		.append("todburl: " + todburl + "\n")
		.append("dburl: " + dburl + "\n")
		.append("commenturl: " + commenturl + "\n")
		.append("reviewsurl: " + reviewsurl + "\n");
		buffer.append("actors: ");
		if(actors!=null){
			for(String text : actors.toStrings()){
				buffer.append(text + "¡¢");
			}
		}
		buffer.append("\n");
		buffer.append("genres: ");
		if(genres!=null){
			for(String text : genres.toStrings()){
				buffer.append(text + "¡¢");
			}
		}
		buffer.append("\n");
		return buffer.toString();
	}




	class TextArrayWritable extends ArrayWritable {

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




	/**
	 * @return the cUR_VERSION
	 */
	public byte getCUR_VERSION() {
		return CUR_VERSION;
	}

	/**
	 * @param cUR_VERSION the cUR_VERSION to set
	 */
	public void setCUR_VERSION(byte cUR_VERSION) {
		CUR_VERSION = cUR_VERSION;
	}

	/**
	 * @return the sTATUS
	 */
	public byte getSTATUS() {
		return STATUS;
	}

	/**
	 * @param sTATUS the sTATUS to set
	 */
	public void setSTATUS(byte sTATUS) {
		STATUS = sTATUS;
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
	 * @return the todburl
	 */
	public String getTodburl() {
		return todburl;
	}

	/**
	 * @param todburl the todburl to set
	 */
	public void setTodburl(String todburl) {
		this.todburl = todburl;
	}

	/**
	 * @return the dburl
	 */
	public String getDburl() {
		return dburl;
	}

	/**
	 * @param dburl the dburl to set
	 */
	public void setDburl(String dburl) {
		this.dburl = dburl;
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
	 * @return the scriptwriter
	 */
	public String getScriptwriter() {
		return scriptwriter;
	}

	/**
	 * @param scriptwriter the scriptwriter to set
	 */
	public void setScriptwriter(String scriptwriter) {
		this.scriptwriter = scriptwriter;
	}

	/**
	 * @return the pubdate
	 */
	public String getPubdate() {
		return pubdate;
	}

	/**
	 * @param pubdate the pubdate to set
	 */
	public void setPubdate(String pubdate) {
		this.pubdate = pubdate;
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
	 * @return the runtime
	 */
	public String getRuntime() {
		return runtime;
	}

	/**
	 * @param runtime the runtime to set
	 */
	public void setRuntime(String runtime) {
		this.runtime = runtime;
	}

	/**
	 * @return the imdbmid
	 */
	public String getImdbmid() {
		return imdbmid;
	}

	/**
	 * @param imdbmid the imdbmid to set
	 */
	public void setImdbmid(String imdbmid) {
		this.imdbmid = imdbmid;
	}

	/**
	 * @return the actorsarry
	 */
	public String[] getActorsarry() {
		return actorsarry;
	}

	/**
	 * @param actorsarry the actorsarry to set
	 */
	public void setActorsarry(String[] actorsarry) {
		this.actorsarry = actorsarry;
		if(actorsarry!=null && actorsarry.length>0){
			actors = new TextArrayWritable(actorsarry);
		}
	}

	/**
	 * @return the genresarry
	 */
	public String[] getGenresarry() {
		return genresarry;
	}

	/**
	 * @param genresarry the genresarry to set
	 */
	public void setGenresarry(String[] genresarry) {
		this.genresarry = genresarry;
		if(genresarry!=null && genresarry.length>0){
			genres = new TextArrayWritable(genresarry);
		}
	}

	/**
	 * @return the commenturl
	 */
	public String getCommenturl() {
		return commenturl;
	}

	/**
	 * @param commenturl the commenturl to set
	 */
	public void setCommenturl(String commenturl) {
		this.commenturl = commenturl;
	}

	/**
	 * @return the reviewsurl
	 */
	public String getReviewsurl() {
		return reviewsurl;
	}

	/**
	 * @param reviewsurl the reviewsurl to set
	 */
	public void setReviewsurl(String reviewsurl) {
		this.reviewsurl = reviewsurl;
	}

	/**
	 * @return the sorce
	 */
	public String getSorce() {
		return sorce;
	}

	/**
	 * @param sorce the sorce to set
	 */
	public void setSorce(String sorce) {
		this.sorce = sorce;
	}
	
	
	
}
