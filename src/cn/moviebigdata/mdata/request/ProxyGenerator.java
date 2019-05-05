package cn.moviebigdata.mdata.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ProxyGenerator {
	
private List<String> proxyContainer = new ArrayList<String>();
	
	private static ProxyGenerator instance;
	
	private ProxyGenerator() {
	}

	public synchronized static ProxyGenerator getInstance(){
		if(instance==null){
			instance = new ProxyGenerator();
		}
		return instance;
	}
	
	public synchronized String[] generate(){
		if(proxyContainer ==null || proxyContainer.size()<=0){
			List<String> proxys = new AutoProxy().createProxy();
			if(proxys == null || proxys.size()<=0){
				System.out.println("proxys loaded failed...");
				return null;
			}else{
				proxyContainer.addAll(proxys);
			}
		}
		Random randomNum = new Random();
		String proxyUlr = proxyContainer.get(randomNum.nextInt(proxyContainer.size()));
		return proxyUlr.split(":");
	}
	
	
	
	public synchronized void deleteproxy(String proxy){
		if(proxyContainer.contains(proxy)){
			proxyContainer.remove(proxy);
		}
	}
	
	public synchronized void clean(){
		proxyContainer = null;
	}
}
