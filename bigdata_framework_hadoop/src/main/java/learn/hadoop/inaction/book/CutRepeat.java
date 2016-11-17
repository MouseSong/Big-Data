package learn.hadoop.inaction.book;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 去除重复数据.
 * 利用hadoop对map输出key的shuffle过程去重.
 * @author zhangdong
 * */
public class CutRepeat extends Configured implements Tool{


	/**
	 * 1.根据InputFormat生产InputSplit
	 * 2.从InputSplit中获取数据的具体信息
	 * 3.根据数据信息从InputFormat获取RecordReader
	 * 4.根据RecordReader的createKey 和 createValue 生成map输入 的key-value
	 * 5.根据map函数处理输入数据
	 * */
	private static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//利用shuffle对key去重
			context.write(value, new Text(""));
		}
	}
	
	/**
	 * 1.Hadoop合并多个map数据
	 * 2.根据分区将合并后的结果输入给reduce函数
	 * 3.根据Reduce函数对数据进行处理
	 * 4.根据OutputFormat输出数据
	 * */
	private static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		    context.write(key, new Text(""));
		}
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf(), "CutRepeatData");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] params = {"hdfs://node1:9000/inaction/cutrepeat/in", 
        "hdfs://node1:9000/inaction/cutrepeat/out/1"};


		Configuration config = new Configuration();
		config.addResource("hadoop-config.xml");
		
		int result = ToolRunner.run(config, new CutRepeat(), params);
		System.exit(result);
	}
}
