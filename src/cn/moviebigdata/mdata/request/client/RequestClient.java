package cn.moviebigdata.mdata.request.client;

import cn.moviebigdata.mdata.response.ClientResponse;

public abstract class RequestClient {
	
	public int READ_TIME_OUT=60*1000*2;
	public int CONN_TIME_OUT=60*1000*2;
	public String PROXY_IP_CACHE;
	public String PROXY_PORT_CACHE;
	
	public abstract ClientResponse request(String url ,Object params);
	
}
