package cn.moviebigdata.mdata.request.client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Properties;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import cn.moviebigdata.mdata.request.ProxyGenerator;
import cn.moviebigdata.mdata.response.ClientResponse;

/**
 * ��Request�Ĵ��·�װ
 * @author Administrator
 *
 */
public class HttpGetRequest extends RequestClient{
	
	private static final Log logger = LogFactory.getLog(HttpGetRequest.class);
	
	@Override
	public ClientResponse request(String url,Object params) {
		ClientResponse result = null;
		HttpClient  httpclient = new HttpClient();
		//���ô����
		String[] proxyUrl = ProxyGenerator.getInstance().generate();
		Properties prop = System.getProperties();
		if(PROXY_IP_CACHE!=null){
			httpclient.getHostConfiguration().setProxy(PROXY_IP_CACHE, Integer.parseInt(PROXY_PORT_CACHE));  
//			httpclient.getParams().setAuthenticationPreemptive(true);
//			httpclient.getState().setProxyCredentials(AuthScope.ANY, new UsernamePasswordCredentials("",""));  
		}else{
			PROXY_IP_CACHE = proxyUrl[0];
			PROXY_PORT_CACHE = proxyUrl[1];
			httpclient.getHostConfiguration().setProxy(PROXY_IP_CACHE, Integer.parseInt(PROXY_PORT_CACHE));  
//			httpclient.getParams().setAuthenticationPreemptive(true);
//			httpclient.getState().setProxyCredentials(AuthScope.ANY, new UsernamePasswordCredentials("",""));  
		}
		httpclient.getHttpConnectionManager().getParams().setConnectionTimeout(CONN_TIME_OUT);
		httpclient.getHttpConnectionManager().getParams().setSoTimeout(READ_TIME_OUT);
		HttpMethod  method = new HeadMethod(url);
		method.addRequestHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		method.addRequestHeader("Connection", "keep-alive");
		//����һ��Ҫ���������Ȼ���Զ���дjava�����кܶ���վ���ж�Ϊ���������
		method.addRequestHeader("User-Agent", "360Spider");
		try {
			result = new ClientResponse();
			int resCode = httpclient.executeMethod(method);
			if(resCode == HttpStatus.SC_OK){
				if("200".equals(method.getStatusCode()+"")){
					result.setCode(method.getStatusLine().getStatusCode()+"");
					result.setContent(method.getResponseBodyAsString());
					result.setProtocol(method.getPath());
					method.releaseConnection();
					httpclient.getHttpConnectionManager().closeIdleConnections(0);
					return result;
				}else if("403".equals(method.getStatusCode()+"")){
					ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
					PROXY_IP_CACHE = null;
					PROXY_PORT_CACHE = null;
					logger.info("REQUESTCODE:\t"+method.getStatusCode()+"\n"+url +"\t ������������");
					method.releaseConnection();
					httpclient.getHttpConnectionManager().closeIdleConnections(0);
					return request(url,null);
				}else {
					PROXY_IP_CACHE = null;
					PROXY_PORT_CACHE = null;
					logger.info("REQUESTCODE:\t"+method.getStatusCode()+"\n"+url +"\t ������������");
					method.releaseConnection();
					httpclient.getHttpConnectionManager().closeIdleConnections(0);
					return request(url,null);
				}
			}else{
				PROXY_IP_CACHE = null;
				PROXY_PORT_CACHE = null;
				logger.info("REQUESTCODE:\t"+method.getStatusCode()+"\n"+url +"\t ������������");
				method.releaseConnection();
				httpclient.getHttpConnectionManager().closeIdleConnections(0);
				return request(url,null);
			}
		} catch(SocketTimeoutException te){
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("SocketTimeoutException:\t"+te.getMessage()+"\n"+url +"\t ������������");
			method.releaseConnection();
			httpclient.getHttpConnectionManager().closeIdleConnections(0);
			return request(url,null);
		} catch(ConnectException ce){
			ProxyGenerator.getInstance().deleteproxy(PROXY_IP_CACHE+":"+PROXY_PORT_CACHE);
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("ConnectException:\t"+ce.getMessage()+"\n"+url +"\t ������������");
			method.releaseConnection();
			httpclient.getHttpConnectionManager().closeIdleConnections(0);
			return request(url,null);
		} catch (SocketException se){ //����Ϊ��ȡ�����ݺ���쳣
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("SocketException:\t" +se.getMessage()+"\n"+url +"\t ������������");
			method.releaseConnection();
			httpclient.getHttpConnectionManager().closeIdleConnections(0);
			return request(url,null);
		} catch (IOException io){ //�������ʵ�IO�쳣
			PROXY_IP_CACHE = null;
			PROXY_PORT_CACHE = null;
			logger.info("IOException:\t" +io.getMessage()+"\n"+url +"\t ������������");
			method.releaseConnection();
			httpclient.getHttpConnectionManager().closeIdleConnections(0);
			return request(url,null);
		} catch (Exception e) {
			logger.info("δ֪�쳣:\t" + e.getMessage() +"\t" + url);
			result = new ClientResponse();
			result.setCode("999999");
			result.setContent(e.getMessage());
			method.releaseConnection();
			httpclient.getHttpConnectionManager().closeIdleConnections(0);
			return result;
		} 
	}
}
