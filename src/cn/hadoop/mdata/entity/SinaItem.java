package cn.hadoop.mdata.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SinaItem implements Writable,Cloneable{
	
	
	public SinaItem() {
		super();
	}

	private String film_id = "-";
	
	private String name = "-";
	
	private String foreign_name = "-";
	
	private String alias = "-";
	
	private String directors = "-";
	
	private String actors = "-";
	
	private String scripters = "-";
	
	private String genre = "-";
	
	private String country = "-";
	
	private String language = "-";
	
	private String play_time = "-";
	
	private String release_date = "-";
	
	private String keyword = "-";
	
	private String weibo_id = "-";
	
	private String imdb_url = "-";
	
	private String imdb_score = "-";
	
	private String douban_url = "-";
	
	private String douban_score = "-";
	
	private String intro = "-";
	
	private String year = "-";
	
	private String create_user = "-";
	
	private String sinaent_url = "-";
	
	private String release_time = "-";
	
	private String poster = "-";

	/**
	 * @return the film_id
	 */
	public String getFilm_id() {
		return film_id;
	}

	/**
	 * @param film_id the film_id to set
	 */
	public void setFilm_id(String film_id) {
		this.film_id = film_id;
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
	 * @return the foreign_name
	 */
	public String getForeign_name() {
		return foreign_name;
	}

	/**
	 * @param foreign_name the foreign_name to set
	 */
	public void setForeign_name(String foreign_name) {
		this.foreign_name = foreign_name;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param alias the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return the directors
	 */
	public String getDirectors() {
		return directors;
	}

	/**
	 * @param directors the directors to set
	 */
	public void setDirectors(String directors) {
		this.directors = directors;
	}

	/**
	 * @return the actors
	 */
	public String getActors() {
		return actors;
	}

	/**
	 * @param actors the actors to set
	 */
	public void setActors(String actors) {
		this.actors = actors;
	}

	/**
	 * @return the scripters
	 */
	public String getScripters() {
		return scripters;
	}

	/**
	 * @param scripters the scripters to set
	 */
	public void setScripters(String scripters) {
		this.scripters = scripters;
	}

	/**
	 * @return the genre
	 */
	public String getGenre() {
		return genre;
	}

	/**
	 * @param genre the genre to set
	 */
	public void setGenre(String genre) {
		this.genre = genre;
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
	 * @return the language
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * @param language the language to set
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 * @return the play_time
	 */
	public String getPlay_time() {
		return play_time;
	}

	/**
	 * @param play_time the play_time to set
	 */
	public void setPlay_time(String play_time) {
		this.play_time = play_time;
	}

	/**
	 * @return the release_date
	 */
	public String getRelease_date() {
		return release_date;
	}

	/**
	 * @param release_date the release_date to set
	 */
	public void setRelease_date(String release_date) {
		this.release_date = release_date;
	}

	/**
	 * @return the keyword
	 */
	public String getKeyword() {
		return keyword;
	}

	/**
	 * @param keyword the keyword to set
	 */
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	/**
	 * @return the weibo_id
	 */
	public String getWeibo_id() {
		return weibo_id;
	}

	/**
	 * @param weibo_id the weibo_id to set
	 */
	public void setWeibo_id(String weibo_id) {
		this.weibo_id = weibo_id;
	}

	/**
	 * @return the imdb_url
	 */
	public String getImdb_url() {
		return imdb_url;
	}

	/**
	 * @param imdb_url the imdb_url to set
	 */
	public void setImdb_url(String imdb_url) {
		this.imdb_url = imdb_url;
	}

	/**
	 * @return the imdb_score
	 */
	public String getImdb_score() {
		return imdb_score;
	}

	/**
	 * @param imdb_score the imdb_score to set
	 */
	public void setImdb_score(String imdb_score) {
		this.imdb_score = imdb_score;
	}

	/**
	 * @return the douban_url
	 */
	public String getDouban_url() {
		return douban_url;
	}

	/**
	 * @param douban_url the douban_url to set
	 */
	public void setDouban_url(String douban_url) {
		this.douban_url = douban_url;
	}

	/**
	 * @return the douban_score
	 */
	public String getDouban_score() {
		return douban_score;
	}

	/**
	 * @param douban_score the douban_score to set
	 */
	public void setDouban_score(String douban_score) {
		this.douban_score = douban_score;
	}

	/**
	 * @return the intro
	 */
	public String getIntro() {
		return intro;
	}

	/**
	 * @param intro the intro to set
	 */
	public void setIntro(String intro) {
		this.intro = intro;
	}

	/**
	 * @return the year
	 */
	public String getYear() {
		return year;
	}

	/**
	 * @param year the year to set
	 */
	public void setYear(String year) {
		this.year = year;
	}

	/**
	 * @return the create_user
	 */
	public String getCreate_user() {
		return create_user;
	}

	/**
	 * @param create_user the create_user to set
	 */
	public void setCreate_user(String create_user) {
		this.create_user = create_user;
	}

	/**
	 * @return the sinaent_url
	 */
	public String getSinaent_url() {
		return sinaent_url;
	}

	/**
	 * @param sinaent_url the sinaent_url to set
	 */
	public void setSinaent_url(String sinaent_url) {
		this.sinaent_url = sinaent_url;
	}

	/**
	 * @return the release_time
	 */
	public String getRelease_time() {
		return release_time;
	}

	/**
	 * @param release_time the release_time to set
	 */
	public void setRelease_time(String release_time) {
		this.release_time = release_time;
	}

	/**
	 * @return the poster
	 */
	public String getPoster() {
		return poster;
	}

	/**
	 * @param poster the poster to set
	 */
	public void setPoster(String poster) {
		this.poster = poster;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		film_id = in.readUTF();
		name = in.readUTF();
		foreign_name = in.readUTF();
		alias = in.readUTF();
		directors = in.readUTF();
		actors = in.readUTF();
		scripters = in.readUTF();
		genre = in.readUTF();
		language = in.readUTF();
		play_time = in.readUTF();
		release_date = in.readUTF();
		keyword = in.readUTF();
		weibo_id = in.readUTF();
		imdb_url = in.readUTF();
		imdb_score = in.readUTF();
		douban_url = in.readUTF();
		douban_score = in.readUTF();
		intro = in.readUTF();
		year = in.readUTF();
		create_user = in.readUTF();
		sinaent_url = in.readUTF();
		release_time = in.readUTF();
		poster = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(film_id);
		out.writeUTF(name);
		out.writeUTF(foreign_name);
		out.writeUTF(alias);
		out.writeUTF(directors);
		out.writeUTF(actors);
		out.writeUTF(scripters);
		out.writeUTF(genre);
		out.writeUTF(language);
		out.writeUTF(play_time);
		out.writeUTF(release_date);
		out.writeUTF(keyword);
		out.writeUTF(weibo_id);
		out.writeUTF(imdb_url);
		out.writeUTF(imdb_score);
		out.writeUTF(douban_url);
		out.writeUTF(douban_score);
		out.writeUTF(intro);
		out.writeUTF(year);
		out.writeUTF(create_user);
		out.writeUTF(sinaent_url);
		out.writeUTF(release_time);
		out.writeUTF(poster);
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("film_id:" + film_id +"\n")
		.append("name:" + name +"\n")
		.append("foreign_name:" + foreign_name +"\n")
		.append("alias:" + alias +"\n")
		.append("directors:" + directors +"\n")
		.append("actors:" + actors +"\n")
		.append("scripters:" + scripters +"\n")
		.append("genre:" + genre +"\n")
		.append("language:" + language +"\n")
		.append("play_time:" + play_time +"\n")
		.append("release_date:" + release_date +"\n")
		.append("keyword:" + keyword +"\n")
		.append("weibo_id:" + weibo_id +"\n")
		.append("imdb_url:" + imdb_url +"\n")
		.append("imdb_score:" + imdb_score +"\n")
		.append("douban_url:" + douban_url +"\n")
		.append("douban_score:" + douban_score +"\n")
		.append("intro:" + intro +"\n")
		.append("year:" + year +"\n")
		.append("create_user:" + create_user +"\n")
		.append("sinaent_url:" + sinaent_url +"\n")
		.append("release_time:" + release_time +"\n")
		.append("poster:" + poster +"\n\n");
		return buffer.toString();
	}
	
	
}
