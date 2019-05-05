package cn.moviebigdata.mdata.reader;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import cn.hadoop.mdata.entity.BTData;

/**
 * 自定义读取多行的reader
 * 
 * @author LIU_SHUAI
 * 
 */
public class BTMuiltLineRecordReader implements RecordReader<Text, BTData> {

	private static final Log LOG = LogFactory
			.getLog(BTMuiltLineRecordReader.class);

	private CompressionCodecFactory compressionCodecs;
	private long start;
	private long pos; // 当前流读入所在的行号
	private long end;
	private LineReader in;
	private CompressionCodec codec;
	private Seekable filePosition;
	private Decompressor decompressor;

	private Text line;// 临时记录读取的行的内容

	// 构造函数中，获取FS的连接，并打开文件流
	public BTMuiltLineRecordReader(Configuration conf, FileSplit split)
			throws IOException {
		compressionCodecs = null;
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(conf);
		// 根据job的配置信息，和split的信息，获取到读取实体文件的信息，这里包括文件的压缩信息。
		// 这里压缩的code有：DEFAULT,GZIP,BZIP2,LZO,LZ4,SNAPPY
		codec = compressionCodecs.getCodec(file);// 得到保存文件的压缩方式
		FileSystem system = file.getFileSystem(conf);
		FSDataInputStream fileIn = system.open(file);

		// 根据不同的压缩方式，设置流的其实读取点
		if (isCompressedInput()) {
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec)
						.createInputStream(
								fileIn,
								decompressor,
								start,
								end,
								org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE.BYBLOCK);
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				in = new LineReader(cIn, conf);// 从新纠正读取位置和借宿位置
				filePosition = cIn;
			} else {
				in = new LineReader(codec.createInputStream(fileIn,
						decompressor), conf);
				filePosition = fileIn;
			}
		} else {// 未压缩时
			fileIn.seek(start);
			in = new LineReader(fileIn, conf);
			filePosition = fileIn;
		}
		if (start != 0L)
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		pos = start;

	}

	private boolean isCompressedInput() {
		return codec != null;
	}

	private int maxBytesToConsume(long pos) {
		return isCompressedInput() ? 2147483647 : (int) Math.min(2147483647L,
				end - pos);
	}

	@Override
	public Text createKey() {
		LOG.info("进入createKey() 方法中....");
		return new Text();
	}

	@Override
	public BTData createValue() {
		LOG.info("进入 createValue() 方法中....");
		return new BTData();
	}

	@Override
	public synchronized long getPos() throws IOException {
		return pos;
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != filePosition)
			retVal = filePosition.getPos();
		else
			retVal = pos;
		return retVal;
	}

	@Override
	public synchronized void close() throws IOException {
		if (in != null)
			in.close();
		if (decompressor != null){
			CodecPool.returnDecompressor(decompressor);
		}
	}

	@Override
	public synchronized float getProgress() throws IOException {
		if (start == end)
			return 0x01;
		else
			return (long) Math.min(1.0F, (long) (getFilePosition() - start)
					/ (long) (end - start));
	}

	// 读取每一行数据的时候，都会执行next()方法。
	// Text bt_key, BTData bt_item两个参数是通过createKey()、和createValue()产生后传递过来的
	@Override
	public boolean next(Text bt_key, BTData bt_item) throws IOException {
		line = new Text();
		int newsize = in.readLine(line);
		while (newsize != 0) {
			if (line.toString().startsWith("genres:")) {
				String bt_genres = line.toString().substring(7,
						line.toString().length());
				if (bt_genres != null && !bt_genres.equals("")) {
					bt_item.setGenresarry(bt_genres.split("、"));
				}
				break;
			} else if (line.toString().contains("CUR_VERSION:")) { // 表示一条数据的解析开始
				String[] meta = line.toString().split("\t");
				bt_item.setMid(meta[0]);
				bt_key.set(meta[0]);
			} else if (line.toString().startsWith("name:")) {
				String[] bt_name = line.toString().split(":");
				bt_item.setName(bt_name[1]);
			} else if (line.toString().startsWith("sorce:")) {
				String[] bt_score = line.toString().split(":");
				bt_item.setSorce(bt_score[1]);
			} else if (line.toString().startsWith("director:")) {
				String bt_director = line.toString().substring(9,
						line.toString().length());
				bt_item.setDirector(bt_director);
			} else if (line.toString().startsWith("scriptwriter:")) {
				String bt_writer = line.toString().substring(13,
						line.toString().length());
				bt_item.setScriptwriter(bt_writer);
			} else if (line.toString().startsWith("pubdate :")) {
				String[] bt_pubdate = line.toString().split(":");
				bt_item.setPubdate(bt_pubdate[1]);
			} else if (line.toString().startsWith("country:")) {
				String[] bt_country = line.toString().split(":");
				bt_item.setCountry(bt_country[1]);
			} else if (line.toString().startsWith("runtime:")) {
				String[] bt_runtime = line.toString().split(":");
				bt_item.setRuntime(bt_runtime[1]);
			} else if (line.toString().startsWith("imdbmid:")) {
				String[] bt_imdbmid = line.toString().split(":");
				bt_item.setImdbmid(bt_imdbmid[1]);
			} else if (line.toString().startsWith("todburl:")) {
				String bt_todburl = line.toString().substring(8,
						line.toString().length());
				bt_item.setTodburl(bt_todburl);
			} else if (line.toString().startsWith("dburl:")) {
				String bt_dburl = line.toString().substring(6,
						line.toString().length());
				bt_item.setDburl(bt_dburl);
			} else if (line.toString().startsWith("commenturl:")) {
				String bt_commenturl = line.toString().substring(11,
						line.toString().length());
				bt_item.setCommenturl(bt_commenturl
						.replace("http://www.bttiantang.com/",
								"http://movie.douban.com").replace(".html", ""));
			} else if (line.toString().startsWith("reviewsurl:")) {
				String bt_reviewsurl = line.toString().substring(11,
						line.toString().length());
				bt_item.setReviewsurl(bt_reviewsurl
						.replace("http://www.bttiantang.com/",
								"http://movie.douban.com").replace(".html", ""));
			} else if (line.toString().startsWith("actors:")) {
				String bt_actors = line.toString().substring(7,
						line.toString().length());
				if (bt_actors != null && !bt_actors.equals("")) {
					bt_item.setActorsarry(bt_actors.split("、"));
				}
			}
			newsize = in.readLine(line);
			if (newsize == 0) {
				return false;
			}
			pos += newsize;
		}
		if (newsize != 0) {
			return true;
		}
		return false;
	}

}
