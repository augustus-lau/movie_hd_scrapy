package cn.moviebigdata.mdata.parse.plugin;

import java.util.List;

import cn.hadoop.mdata.exception.NoneFetchItemException;

public abstract class ParseHtml<E> {
	
	/**
	 * 解析内容，返回想要的结果
	 * @param content
	 * @return
	 */
	public abstract E parse(String content);
	
	
	/**
	 * 解析内容，返回想要的结果集
	 * @param content
	 * @return
	 */
	public abstract List<E> parsetoList(String content,String _key) throws NoneFetchItemException;
	
	/**
	 * 分页
	 * 判断是否还有下一页
	 * @param end_regex
	 * @return
	 */
	public abstract boolean hasNextPage(int curpage);
	
	
	public abstract int nextPage();
}
