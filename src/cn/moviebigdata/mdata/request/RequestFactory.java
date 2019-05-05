package cn.moviebigdata.mdata.request;


public class RequestFactory {
	
	public static Request createRequest(Class<? extends Request> cla,String url){
		Request request = null;
		try {
			request = (Request) Class.forName(cla.getName()).newInstance();
			if(request!=null){
				request.buildRequest(url);
			}else{
				throw new Exception("cann't instance Class :" + cla.getName());
			}
		} catch (InstantiationException e) {
			System.out.println("InstantiationException :" + cla.getName());
		} catch (IllegalAccessException e) {
			System.out.println("IllegalAccessException :" + cla.getName());
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFoundException :" + cla.getName());
		} catch (Exception e) {
			System.out.println("cann't instance Class :" + cla.getName());
		}
		return request;
	}
	
	
}
