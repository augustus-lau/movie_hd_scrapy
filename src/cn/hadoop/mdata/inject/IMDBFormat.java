package cn.hadoop.mdata.inject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import cn.hadoop.mdata.entity.MData;
import cn.moviebigdata.mdata.inputformat.IMDBInputFormat;
import cn.moviebigdata.mdata.outputformat.IMDBOutputFormat;

/**
 * 格式化IMDB的数据
 * @author Administrator
 *
 */
public class IMDBFormat extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: IMDBFormat <sourcePath> <resultPath>");
			return -1;
		}
		try {
			from(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			System.err.println("IMDBFormat: "
					+ StringUtils.stringifyException(e));
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IMDBFormat(), args);
		System.exit(res);
	}

	private void from(Path from, Path to) throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long start = System.currentTimeMillis();
		System.out.println("IMDBFormat: starting at " + sdf.format(start));
		System.out.println("IMDBFormat: source: " + from);
		System.out.println("IMDBFormat: result: " + to);
		Configuration conf = this.getConf();
		JobConf washJob = new JobConf(conf);
		washJob.setJobName("IMDBFormat");
		FileInputFormat.addInputPath(washJob, from);
//		FileInputFormat.addInputPath(washJob, new Path("F:/nutch2.3/movie/imdb_2003_pre_hadoop_fmt/part-00000"));
		FileOutputFormat.setOutputPath(washJob, to);

		washJob.setMapperClass(FormatMapper.class);
		washJob.setReducerClass(FormatReducer.class);

		washJob.setInputFormat(IMDBInputFormat.class);
		washJob.setOutputFormat(IMDBOutputFormat.class);

		washJob.setMapOutputKeyClass(Text.class);
		washJob.setMapOutputValueClass(MData.class);

		washJob.setOutputKeyClass(Text.class);
		washJob.setOutputValueClass(MData.class);
		JobClient.runJob(washJob);
	}

	public static class FormatMapper extends MapReduceBase implements
			Mapper<Text, MData, Text, MData> {

		@Override
		public void map(Text mid, MData item,
				OutputCollector<Text, MData> output, Reporter reporter)
				throws IOException {
			output.collect(mid, item);
		}

	}

	public static class FormatReducer extends MapReduceBase implements
			Reducer<Text, MData, Text, MData> {

		@Override
		public void reduce(Text mid, Iterator<MData> items,
				OutputCollector<Text, MData> output, Reporter reporter)
				throws IOException {
			MData first = null;
			while (items.hasNext()) {
				first = items.next();
				break;
			}
			if (first != null) {
				output.collect(mid, first);
			} else {
				return;
			}
		}

	}

}
