package cn.moviebigdata.mdata.request;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class Takser extends Thread {
	
	private static final Log LOG = LogFactory.getLog(Takser.class);
	
	public Takser() {
		this.setDaemon(true);
		LOG.info("initing thread   -threadname : " + this.getName());
		
	}

	@Override
	public void run() {
		begin();
	}
	
	public abstract void begin();
	
}
