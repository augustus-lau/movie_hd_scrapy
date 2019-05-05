package cn.moviebigdata.mdata.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import cn.hadoop.mdata.entity.MData;

public class IMDBRecordReader implements RecordReader<Text, MData> {

	private LineReader in;
	private long start;
	private long pos;
	private long end;

	public IMDBRecordReader(FileSplit split, Configuration conf)
			throws IOException {
		start = split.getStart();
		end = start + split.getLength();
		pos = start;

		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(path);
		fileIn.seek(start);
		in = new LineReader(fileIn, conf);

		// 如果开始位置不为0，那么就再新读取一行，但是读取长度为0，表示将起始点置到下一行的开始位置上去
		if (start != 0L)
			start += in.readLine(new Text(), 0, 2147483647);
		pos = start;
	}

	@Override
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public MData createValue() {
		return new MData();
	}

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	@Override
	public synchronized float getProgress() throws IOException {
		if (start == end)
			return 0x01;
		else
			return (long) Math.min(1.0F, (long) (pos - start)
					/ (long) (end - start));
	}

	@Override
	public boolean next(Text _key, MData _item) throws IOException {
		Text line = new Text();
		int newsize = in.readLine(line);
		String content = line.toString();
		while (newsize != 0) {
			if (content.contains("mid:")) {
				String[] arr_mid = content.split(":");
				_item.setMid(arr_mid[1]);
			} else if (content.startsWith("name:")) {
				String[] arr_name = content.split(":");
				_item.setName(arr_name[1]);
			} else if (content.startsWith("seri:")) {
				String[] arr_seri = content.split(":");
				_item.setSeri(arr_seri[1]);
			} else if (content.startsWith("episode:")) {
				String[] arr_episode = content.split(":");
				_item.setEpisode(arr_episode[1]);
			} else if (content.startsWith("rata:")) {
				String[] arr_rata = content.split(":");
				_item.setRata(arr_rata[1]);
			} else if (content.startsWith("runtime:")) {
				String[] arr_time = content.split(":");
				_item.setRuntime(arr_time[1]);
			} else if (content.startsWith("url:")) {
				_item.setUrl(content.substring(4, content.length()));
			} else if (content.startsWith("thumb:")) {
				_item.setThumb(content.substring(6, content.length()));
			} else if (content.startsWith("actors:")) {
				String[] actorsarry = content.split("、");
				_item.setActorsarry(actorsarry);
			}else if (content.startsWith("genres:")) {
				String[] genresarry = content.split("、");
				_item.setGenresarry(genresarry);
				break;
			}
			newsize = in.readLine(line);
			content = line.toString();
			pos += newsize;
			if(newsize == 0){
				break;
			}
		}
		_key.set(_item.getMid());
		if(newsize != 0){
			return true;
		}
		
		return false;
	}


}
