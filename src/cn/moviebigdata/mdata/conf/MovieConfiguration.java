package cn.moviebigdata.mdata.conf;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * 利用hadoop加载自身的配置文件,并合并自己的配置文件到hadoop
 * 
 * @author Administrator
 * 
 */
public class MovieConfiguration extends Configuration{
	
	public static final String UUID_KEY = "movie.conf.uuid";
	
	private MovieConfiguration(Configuration conf,String taskName) {
		conf.set("M_TASK_NAME", taskName);
	}

	private static void setUUID(Configuration conf) {
		UUID uuid = UUID.randomUUID();
		conf.set(UUID_KEY, uuid.toString());
	}

	private static Configuration addNutchResources(Configuration conf) {
		conf.addResource("mdata-default.xml");
		return conf;
	}
	
	/**
	 * 公开调用方法，创建hadoop的配置文件Configuration，并将自己创建的配置文件合并到hadoop
	 */
	public static Configuration create() {
		Configuration conf = new Configuration();
		setUUID(conf);
		addNutchResources(conf);
		return conf;
	}
}
