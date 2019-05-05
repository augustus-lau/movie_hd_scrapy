package cn.moviebigdata.mdata.request;

public abstract class Request {
	
	public int READ_TIME_OUT=60*1000*2;
	public int CONN_TIME_OUT=60*1000*2;
	public String PROXY_IP_CACHE;
	public String PROXY_PORT_CACHE;
	public  String url_path;
	
	public abstract String request();
	
	
	public void buildRequest(String url){
		this.url_path = url;
	}
}
