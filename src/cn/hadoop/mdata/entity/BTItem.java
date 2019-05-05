package cn.hadoop.mdata.entity;

public class BTItem {
	
	private String mname;
	
	private String oname;
	
	private String url;
	
	private String todburl;
	
	private byte fetchStatus;
	
	private String modifytime;
	
	/**
	 * @return the fetchStatus
	 */
	public byte getFetchStatus() {
		return fetchStatus;
	}

	/**
	 * @param fetchStatus the fetchStatus to set
	 */
	public void setFetchStatus(byte fetchStatus) {
		this.fetchStatus = fetchStatus;
	}

	/**
	 * @return the modifytime
	 */
	public String getModifytime() {
		return modifytime;
	}

	/**
	 * @param modifytime the modifytime to set
	 */
	public void setModifytime(String modifytime) {
		this.modifytime = modifytime;
	}

	/**
	 * @return the mname
	 */
	public String getMname() {
		return mname;
	}

	/**
	 * @param mname the mname to set
	 */
	public void setMname(String mname) {
		this.mname = mname;
	}

	/**
	 * @return the oname
	 */
	public String getOname() {
		return oname;
	}

	/**
	 * @param oname the oname to set
	 */
	public void setOname(String oname) {
		this.oname = oname;
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
	
	
	
}
