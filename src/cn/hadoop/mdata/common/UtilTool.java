package cn.hadoop.mdata.common;

public class UtilTool {
	
	
	public static String join(String spator,String [] data){
		if(data!=null && data.length>0){
			StringBuffer buffer = new StringBuffer();
			for(String temp : data){
				if(temp!=null &&!"".equals(temp)){
					buffer.append(temp).append(spator);
				}
			}
			String result = buffer.toString();
			return result.substring(0, result.length()-1);
		}
		return "-";
	}
}
