package cn.hadoop.mdata.inject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.hadoop.mdata.entity.MData;
public class IMDBInjector extends Configured implements Tool {
	
	
	 private final static byte CUR_VERSION = 7;
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: injector <sourcePath> <resultPath>");
			return -1;
		}
		try {
			inject(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("Injector: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IMDBInjector(), args);
		System.exit(res);
	}
	
	
	
	public void inject(Path source, Path result) throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		System.out.println("Injector: starting at " + sdf.format(start));
		System.out.println("Injector: source: " + source);
		System.out.println("Injector: result: " + result);
		Configuration conf = this.getConf();
		JobConf sortJob = new JobConf(conf);
		sortJob.setJobName("inject-mdata");
		
		sortJob.setInputFormat(TextInputFormat.class);
		sortJob.setOutputFormat(TextOutputFormat.class);
		
		sortJob.setOutputKeyClass(Text.class);
		sortJob.setOutputValueClass(MData.class);
		
		sortJob.setMapperClass(InjectMapper.class);
		sortJob.setReducerClass(InjectReduce.class);
		
		FileInputFormat.setInputPaths(sortJob, source);
		FileOutputFormat.setOutputPath(sortJob, result);
		
		JobClient.runJob(sortJob);
	}

	public static class InjectMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, MData> {
		
		private static final String item_name_img= "<tdclass=\"image\"><ahref=\"(.*?)\"title=\"(.*?)\"><imgsrc=\"(.*?)\"height=\"(.*?)\"width=\"(.*?)\"alt=\"(.*?)\"title=\"(.*?)\"></a></td>";
		
		//所属的电视剧
		private static final String tv_seri="<spanclass=\"episode\">Episode:<ahref=\"(.*?)\">(.*?)</a>(.*?)</span>";
		
		//投票
		private static final String m_rate="<spanclass=\"rating-rating\"><spanclass=\"value\">(.*?)</span>";
		
		//演员
		private static final String actor="<spanclass=\"credit\">(.*?)</span>";
		
		//题材
		private static final String genre="<spanclass=\"genre\">(.*?)</span>";
		
		//时长
		private static final String runtime = "<spanclass=\"runtime\">(.*?)</span>";
		
		private MData mData;
		
		@Override
		public void map(LongWritable offset, Text value,
				OutputCollector<Text, MData> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String nstr = parseMname(line);
			String mid = parseMid(line);
			String seri = parseSeri(line);
			String[] actors = parseactor(line);
			String[] genre = parsegenre(line);
			String rate = parserate(line);
			String run_time = parseTime(line);
			if(mid==null ||"".equals(mid)){
				return;
			}
			
			if(nstr!=null && !"".equals(nstr)){
				String [] temp = nstr.split(",");
				mData = new MData(CUR_VERSION, mid, seri, rate, run_time, temp[0], temp[1], temp[2], seri, actors, genre);
			}else{
				return;
			}
			
			output.collect(new Text(mid),mData);
		}

		private String parseMname(String line) {
			if (line != null) {
				Pattern pattern = Pattern.compile(item_name_img);
				Matcher matcher = pattern.matcher(line);
				StringBuffer buffer = new StringBuffer();
				while (matcher.find()) {
					buffer.append(matcher.group(1)).append(",")
							.append(matcher.group(2)).append(",")
							.append(matcher.group(3));
				}
				return buffer.toString();
			}
			return null;
		}
		
		/**
		 * 解析剧长
		 * @param line
		 * @return
		 */
		private String parseTime(String line){
			if (line != null) {
				Pattern pattern = Pattern.compile(runtime);
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					String seri = matcher.group(1);
					if(seri!=null && !"".equals(seri)){
						return seri;
					}
				}
			}
			return null;
		}
		
		/**
		 * 解析所属电视剧
		 * @param line
		 * @return
		 */
		private String parseSeri(String line){
			if (line != null) {
				Pattern pattern = Pattern.compile(tv_seri);
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					String seri = matcher.group(3);
					if(seri!=null && !"".equals(seri)){
						return seri;
					}
				}
			}
			return null;
		}
		
		
		private String parseMid(String line){
			if (line != null) {
				Pattern pattern = Pattern.compile(item_name_img);
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					String title = matcher.group(1);
					if(title!=null && !"".equals(title)){
						String mid = title.substring(title.indexOf("tt"), title.length()-1);
						return mid;
					}
				}
			}
			return null;
		}
		
		/**
		 * 解析电影的评价
		 * @param line
		 * @return
		 */
		private String parserate(String line){
			if (line != null) {
				Pattern pattern = Pattern.compile(m_rate);
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					String rate = matcher.group(1);
					if(rate!=null && !"".equals(rate)){
						return rate;
					}
				}
			}
			return null;
		}
		/**
		 * 解析演员
		 * @param line
		 * @return
		 */
		private String[] parseactor(String line){
			String actor_regx = "<ahref=\"(.*?)\">(.*?)</a>";
			List<String> actors = new ArrayList<String>();
			String [] result = null;
			if (line != null) {
				Pattern pattern = Pattern.compile(actor);
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					String actor = matcher.group(1);
					if(actor!=null && !"".equals(actor)){
						Pattern ap = Pattern.compile(actor_regx);
						Matcher am = ap.matcher(actor);
						while (am.find()) {
							actors.add(am.group(2));
							actors.add(am.group(1));
						}
					}
				}
			}
			if(actors.size()>0){
				result = new String[actors.size()];
				for(int i =0;i<actors.size();i++){
					result[i] = actors.get(i);
				}
				return result;
			}
			return null;
		}
		
		/**
		 * 解析题材
		 * @param line
		 * @return
		 */
		private String[] parsegenre(String line){
			String genre_regx = "<ahref=\"(.*?)\">(.*?)</a>";
			List<String> genres = new ArrayList<String>();
			String [] result = null;
			if (line != null) {
				Pattern pattern = Pattern.compile(genre);
				Matcher matcher = pattern.matcher(line);
				while (matcher.find()) {
					String genre = matcher.group(1);
					if(genre!=null && !"".equals(genre)){
						if(genre!=null && !"".equals(genre)){
							Pattern ap = Pattern.compile(genre_regx);
							Matcher am = ap.matcher(genre);
							while (am.find()) {
								genres.add(am.group(2));
								genres.add(am.group(1));
							}
						}
					}
				}
			}
			if(genres.size()>0){
				result = new String[genres.size()];
				for(int i =0;i<genres.size();i++){
					result[i] = genres.get(i);
				}
				return result;
			}
			return null;
		}
	}
	
	public static class InjectReduce extends MapReduceBase implements
			Reducer<Text, MData, Text, MData> {

		@Override
		public void reduce(Text mid, Iterator<MData> mdatas,
				OutputCollector<Text, MData> output, Reporter arg3)
				throws IOException {
			
			MData first = null;
			while (mdatas.hasNext()) {
				if(first == null){
					first = mdatas.next();
				}else{
					MData other = mdatas.next();
					first.addActors(other);
					first.addGenres(other);
				}
			}
			output.collect(mid, first);
		}
		
		
	}

}
