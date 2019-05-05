package cn.hadoop.mdata.token;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TokenUtil {
	
	public static List<String> tokenToTerm(String content,Analyzer a){
		List<String> terms = new ArrayList<String>();
		try {
			TokenStream stream = a.tokenStream("comment", new StringReader(content));
			CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
			stream.reset();
			while(stream.incrementToken()){
				terms.add(term.toString());
			}
			stream.close();
			return terms;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
