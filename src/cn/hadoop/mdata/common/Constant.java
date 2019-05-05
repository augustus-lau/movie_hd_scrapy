package cn.hadoop.mdata.common;

public class Constant {
	
	public static final byte FETCH_VERSION=0x01;
	
	public static final byte FETCH_DONE = 0x00;
	
	public static final byte FETCH_EXCEPTION = 0x01;
	
	public static final byte FETCH_NETWORK_ERROR = 0x02;

	public static final byte FETCH_END_SIGNAL = 0x03;
	
	/**
	 * 数据解析过程中产生的异常
	 */
	public static final byte FETCH_PARSE_ERROR = 0x04;
	
	
	public static final byte FETCH_LEVEL_INJECT = 0x01;
	
	public static final byte FETCH_LEVEL_FILLING = 0x02;
	
	public static final byte FETCH_LEVEL_REPLY = 0x03;
	
	public static final byte FETCH_LEVEL_USER = 0x04;
	
	
	public static final String FETCH_MSG_NULL = "_NULL";
	
	public static final String FETCH_MSG_ERROR = "_ERROR";
	
	public static final String FETCH_MSG_SUCCESS = "_SUCCESS";
}
