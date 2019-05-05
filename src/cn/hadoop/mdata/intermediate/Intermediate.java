package cn.hadoop.mdata.intermediate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;


/**
 * ¼ÇÂ¼ÒÇ
 * @author tk
 *
 */
public class Intermediate {
	
	
	private static Intermediate instance;
	
	private BufferedWriter writer;
	
	private Intermediate(String filepath) throws IOException {
		if(filepath == null || "".equals(filepath)){
			throw new FileNotFoundException("Intermediate File cannot be null!");
		}
		
		File file = new File(filepath);
		if(!file.exists()){
			file.createNewFile();
		}
		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
	}
	
	
	
	public static synchronized Intermediate getIntanace(String filepath) throws IOException{
		if(instance == null){
			instance = new Intermediate(filepath);
		}
		return instance;
	};
	
	public synchronized void write(String content) throws IOException{
		writer.append(content);
		writer.newLine();
		writer.flush();
	}
	
	public synchronized void close() throws IOException{
		if(writer!=null){
			writer.close();
		}
	}
	
}
