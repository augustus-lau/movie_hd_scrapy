package cn.moviebigdata.mdata.parse.plugin;

import java.util.List;

import cn.hadoop.mdata.exception.NoneFetchItemException;

public abstract class ParseHtml<E> {
	
	/**
	 * �������ݣ�������Ҫ�Ľ��
	 * @param content
	 * @return
	 */
	public abstract E parse(String content);
	
	
	/**
	 * �������ݣ�������Ҫ�Ľ����
	 * @param content
	 * @return
	 */
	public abstract List<E> parsetoList(String content,String _key) throws NoneFetchItemException;
	
	/**
	 * ��ҳ
	 * �ж��Ƿ�����һҳ
	 * @param end_regex
	 * @return
	 */
	public abstract boolean hasNextPage(int curpage);
	
	
	public abstract int nextPage();
}
