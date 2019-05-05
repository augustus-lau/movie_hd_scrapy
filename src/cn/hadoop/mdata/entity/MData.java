package cn.hadoop.mdata.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MData implements WritableComparable<MData>, Cloneable {

	private byte CUR_VERSION = 1;

	private String mid, seri, rata, runtime, url, name, thumb, episode;

	private String[] actorsarry, genresarry;

	private ArrayWritable actors, genres;

	public MData() {
		mid = "-";
		seri = "-";
		rata = "-";
		runtime = "-";
		url = "-";
		name = "-";
		thumb = "-";
		episode = "-";
	}

	public MData(byte CUR_VERSION, String mid, String seri, String rata,
			String runtime, String url, String name, String thumb,
			String episode, String[] actorsarry, String[] genresarry) {
		super();
		this.CUR_VERSION = CUR_VERSION;
		this.mid = mid;
		if(seri==null || "".equals(seri)){
			this.seri = "-";
		}else{
			this.seri = seri;
		}
		
		if(rata==null || "".equals(rata)){
			this.rata = "-";
		}else{
			this.rata = rata;
		}
		if(runtime==null || "".equals(runtime)){
			this.runtime = "-";
		}else{
			this.runtime = runtime;
		}
		if(url==null || "".equals(url)){
			this.url = "-";
		}else{
			this.url = url;
		}
		if(name==null || "".equals(name)){
			this.name = "-";
		}else{
			this.name = name;
		}
		if(thumb==null || "".equals(thumb)){
			this.thumb = "-";
		}else{
			this.thumb = thumb;
		}
		if(episode==null || "".equals(episode)){
			this.episode = "-";
		}else{
			this.episode = episode;
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
	
	public void addActors(MData other){
		actors.set(other.getActors().get());
	}
	
	public void addGenres(MData other){
		genres.set(other.getGenres().get());
	}
	
	@Override
	public void write(DataOutput dataoutput) throws IOException {
		dataoutput.writeByte(CUR_VERSION);
		dataoutput.writeUTF(mid);
		dataoutput.writeUTF(seri);
		dataoutput.writeUTF(rata);
		dataoutput.writeUTF(runtime);
		dataoutput.writeUTF(url);
		dataoutput.writeUTF(name);
		dataoutput.writeUTF(thumb);
		dataoutput.writeUTF(episode);
		if(actors!=null){
			dataoutput.writeBoolean(true);
			actors.write(dataoutput);
		}else{
			dataoutput.writeBoolean(false);
		}
		if(genres!=null){
			dataoutput.writeBoolean(true);
			genres.write(dataoutput);
		}else{
			dataoutput.writeBoolean(false);
		}
	}

	@Override
	public void readFields(DataInput datainput) throws IOException {
		CUR_VERSION = datainput.readByte();
		mid = datainput.readUTF();
		seri = datainput.readUTF();
		rata = datainput.readUTF();
		runtime = datainput.readUTF();
		url = datainput.readUTF();
		name = datainput.readUTF();
		thumb = datainput.readUTF();
		episode = datainput.readUTF();
		boolean hasActors = datainput.readBoolean();
		if(hasActors){
			actors = new TextArrayWritable(Text.class);
			actors.readFields(datainput);
		}
		boolean hasGenres = datainput.readBoolean();
		if(hasGenres){
			genres = new TextArrayWritable(Text.class);
			genres.readFields(datainput);
		}
	}

	/**
	 * 这个方法是按照如何将数据排序 这里采用mid的降速排列的方式 该对象的mid与其他对象比较时，如果大，则返回1，小则返回-1
	 */
	@Override
	public int compareTo(MData that) {
		String thatmid = that.mid.replace("tt", "");
		String thismid = this.mid.replace("tt", "");
		return (Integer.parseInt(thatmid) - Integer.parseInt(thismid)) > 0 ? 1
				: -1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Version: " +CUR_VERSION+"\n")
		.append("mid: " +mid + "\n")
		.append("name: " +name + "\n")
		.append("seri: " +seri + "\n")
		.append("episode: " +episode + "\n")
		.append("rata: " +rata + "\n")
		.append("runtime: " +runtime + "\n")
		.append("url: " +url + "\n")
		.append("thumb: " +thumb + "\n")
		.append("actors: ");
		if(actors!=null){
			for(String text : actors.toStrings()){
				buffer.append(text + "、");
			}
		}
		buffer.append("\n");
		buffer.append("genres: ");
		if(genres!=null){
			for(String text : genres.toStrings()){
				buffer.append(text + "、");
			}
		}
		buffer.append("\n");
		return buffer.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
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
	 * @return the seri
	 */
	public String getSeri() {
		return seri;
	}

	/**
	 * @param seri the seri to set
	 */
	public void setSeri(String seri) {
		this.seri = seri;
	}

	/**
	 * @return the rata
	 */
	public String getRata() {
		return rata;
	}

	/**
	 * @param rata the rata to set
	 */
	public void setRata(String rata) {
		this.rata = rata;
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
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
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
	 * @return the thumb
	 */
	public String getThumb() {
		return thumb;
	}

	/**
	 * @param thumb the thumb to set
	 */
	public void setThumb(String thumb) {
		this.thumb = thumb;
	}

	/**
	 * @return the episode
	 */
	public String getEpisode() {
		return episode;
	}

	/**
	 * @param episode the episode to set
	 */
	public void setEpisode(String episode) {
		this.episode = episode;
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
	 * @return the actors
	 */
	public ArrayWritable getActors() {
		return actors;
	}

	/**
	 * @param actors the actors to set
	 */
	public void setActors(ArrayWritable actors) {
		this.actors = actors;
	}

	/**
	 * @return the genres
	 */
	public ArrayWritable getGenres() {
		return genres;
	}

	/**
	 * @param genres the genres to set
	 */
	public void setGenres(ArrayWritable genres) {
		this.genres = genres;
	}
	
	
	
	
}
