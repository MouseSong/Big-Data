package learn.hadoop.inaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * 排序.
 * hadoop的shuffle过程会默认对key进行排序，
 * Text按照字典属性，IntWritable按照数字大写。
 * 也可以自定义排序函数。
 * */
public class Sort extends Configured implements Tool{

	/**
	 * 1.根据InputFormat生产InputSplit
	 * 2.从InputSplit中获取数据的具体信息
	 * 3.根据数据信息从InputFormat获取RecordReader
	 * 4.根据RecordReader的createKey 和 createValue 生成map输入 的key-value
	 * 5.根据map函数处理输入数据
	 * */
	private static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			try{
				IntWritable number = new IntWritable(Integer.parseInt(value.toString()));
			    context.write(number, new Text(""));
			}catch(NumberFormatException e){
				//跳过不能转换的
				System.out.println(e.getMessage());
			}
		}
	}
	
	/**
	 * 1.Hadoop合并多个map数据
	 * 2.根据分区将合并后的结果输入给reduce函数
	 * 3.根据Reduce函数对数据进行处理
	 * 4.根据OutputFormat输出数据
	 * */
	private static class ReduceClass extends Reducer<IntWritable, Text, IntWritable, IntWritable>{
		//全局计数器
		private static IntWritable lineNum = new IntWritable(1);
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
             context.write(lineNum, key);	
             
             //计数器+1
             lineNum = new IntWritable(lineNum.get() +1 );
		}
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf(), "Sort");
		job.setJarByClass(Sort.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] params = {"hdfs://node1:9000/inaction/sort/in", 
        "hdfs://node1:9000/inaction/sort/out/1"};


		Configuration config = new Configuration();
		config.addResource("hadoop-config.xml");
		
		int result = ToolRunner.run(config, new Sort(), params);
		System.exit(result);
	}
}
