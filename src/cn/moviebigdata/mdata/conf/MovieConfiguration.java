package cn.moviebigdata.mdata.conf;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

/**
 * 
 * ����hadoop��������������ļ�,���ϲ��Լ��������ļ���hadoop
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
	 * �������÷���������hadoop�������ļ�Configuration�������Լ������������ļ��ϲ���hadoop
	 */
	public static Configuration create() {
		Configuration conf = new Configuration();
		setUUID(conf);
		addNutchResources(conf);
		return conf;
	}
}
