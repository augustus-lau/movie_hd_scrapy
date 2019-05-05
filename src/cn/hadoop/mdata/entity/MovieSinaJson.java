package cn.hadoop.mdata.entity;

import java.util.List;

public class MovieSinaJson {
	
	private String error_code;
	
	private DataFormat data;
	
	/**
	 * @return the error_code
	 */
	public String getError_code() {
		return error_code;
	}


	/**
	 * @param error_code the error_code to set
	 */
	public void setError_code(String error_code) {
		this.error_code = error_code;
	}


	/**
	 * @return the data
	 */
	public DataFormat getData() {
		return data;
	}




	/**
	 * @param data the data to set
	 */
	public void setData(DataFormat data) {
		this.data = data;
	}




	public class DataFormat{
		
		private List<SinaItem>  list;
		
		private int total;

		/**
		 * @return the list
		 */
		public List<SinaItem> getList() {
			return list;
		}

		/**
		 * @param list the list to set
		 */
		public void setList(List<SinaItem> list) {
			this.list = list;
		}

		/**
		 * @return the total
		 */
		public int getTotal() {
			return total;
		}

		/**
		 * @param total the total to set
		 */
		public void setTotal(int total) {
			this.total = total;
		}
		
		
		
	}
}
