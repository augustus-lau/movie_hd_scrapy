package cn.hadoop.mdata.intermediate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class IntermediateReader {

	
	private static IntermediateReader instance;
	
	private BufferedReader reader;
	
	public IntermediateReader(String path) throws IOException {
		if(path == null || "".equals(path)){
			throw new FileNotFoundException("IntermediateReader cannot be null");
		}
		
		File file = new File(path);
		if(!file.exists()){
			file.createNewFile();
		}
		
		reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}




	public static synchronized IntermediateReader getInstance(String path) throws IOException{
		if(instance == null){
			instance = new IntermediateReader(path);
		}
		return instance;
	}
	
	public Set<String> read() throws IOException{
		Set<String> feched = new HashSet<String>();
		String line =null;
		while((line=reader.readLine())!=null)
         {
			 feched.add(line.trim());
         }

		reader.close();
		if(feched == null || feched.size() <=0){
			return null;
		}
		return feched;
	}
}
