package cn.hadoop.movie.analyse.db.actors;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import cn.hadoop.mdata.entity.DBDYData;
import cn.moviebigdata.mdata.conf.MovieConfiguration;
import cn.moviebigdata.mdata.outputformat.AVGScoreOutputFormat;

public class ActorScore extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: ActorScore <sourcePath> <resultPath>");
			return -1;
		}
		try {
			actorscore(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("ActorScore: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}
	
	private void actorscore(Path from,Path to) throws IOException{
		Configuration conf = this.getConf();
		JobConf dumpjob = new JobConf(conf);
		dumpjob.setJobName("actorscore");
		
		FileInputFormat.addInputPath(dumpjob, from);
//		FileOutputFormat.setOutputPath(dumpjob, to);
		
		dumpjob.setMapperClass(ScoreMapper.class);
		dumpjob.setReducerClass(ScoreReducer.class);
		
		dumpjob.setMapOutputKeyClass(Text.class);
		dumpjob.setMapOutputValueClass(Text.class);
		
		dumpjob.setOutputKeyClass(FloatWritable.class);
		dumpjob.setOutputValueClass(Text.class);
		
		dumpjob.setInputFormat(SequenceFileInputFormat.class);
		dumpjob.setOutputFormat(AVGScoreOutputFormat.class);
		JobClient.runJob(dumpjob);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(MovieConfiguration.create(), new ActorScore(), args);
		System.exit(res);
	}
	
	
	private static class ScoreMapper extends MapReduceBase implements Mapper<Text, DBDYData, Text, Text>{
		@Override
		public void map(Text _key, DBDYData _data,
				OutputCollector<Text, Text> out, Reporter reporter)
				throws IOException {
			String[] actors = _data.getActors_text().toStrings();
			if(actors!=null && actors.length >0){
				for(String actor : actors){
					out.collect(new Text(actor), new Text(_data.getRating()+"\t" +_data.getCountry()));
				}
			}
		}
	}
	
	private static class ScoreReducer extends MapReduceBase implements Reducer<Text, Text, FloatWritable, Text>{

		@Override
		public void reduce(Text _actor, Iterator<Text> _datas,
				OutputCollector<FloatWritable, Text> out, Reporter arg3)
				throws IOException {
			int mcount = 0;
			String countrys = null;
			Set<String> country = new HashSet<String>();
			float sumScore = 0;
			StringBuffer scores= new StringBuffer();
			while(_datas.hasNext()){
				String _value = _datas.next().toString();
				String[] values = _value.split("\t");
				if(values[1]!=null && !"".equals(values[1])){
					if(!country.contains(values[1])){
						country.add(values[1]);
					}
				}
				
				if(values[0]!=null && !"".equals(values[0]) && !"-".equals(values[0])){
					sumScore = sumScore + Float.parseFloat(values[0]);
				}
				scores.append(values[0]).append(",");
				mcount ++;
			}
			StringBuffer cbuffer = null;
			if(mcount>=3){
				if(country.size()>0){
					cbuffer = new StringBuffer();
					Iterator<String> it = country.iterator();
					if(it.hasNext()){
						cbuffer.append(it.next()).append(",");
					}
					countrys = cbuffer.toString();
				}
				
				//如果直接计算平均数，会导致积分肯定不准，因为 只有有价值的演员，导演才会去找他排电影，所以电影数量也是影响演员价值的考核工具
				float avg = sumScore/mcount;
				out.collect(new FloatWritable(avg), new Text(mcount +"\t"+_actor.toString() +"\t" +scores.toString()+"\t" +countrys));
			}
			
		}

		
	}
	
	
}
