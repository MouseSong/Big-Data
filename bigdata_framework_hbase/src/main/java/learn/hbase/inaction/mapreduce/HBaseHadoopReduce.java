package learn.hbase.inaction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import learn.hbase.inaction.Infos;

/**
 * Calculate word count map reduce, and store result into hbase
 * @author zhangdong
 * @createtime 1026-10-31
 * @location peking
 * */
class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public MapClass() {
		// TODO Auto-generated constructor stub
		System.out.println("MapClass Constructor");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for(String word : words){
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
public class HBaseHadoopReduce extends Configured implements Tool{
	
	
	
	private class HBaseReduceClass extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			int count = 0;
			for(IntWritable value : values){
				count += value.get();
			}
			
			Put put = new Put(("wc-" + key.toString()).getBytes());
			put.add("info".getBytes(), "word".getBytes(), key.getBytes());
			put.add("info".getBytes(), "count".getBytes(), String.valueOf(count).getBytes());
			
			context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
		}

		
		
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf(), "HBaseHadoopReduce");
		
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		TableMapReduceUtil.initTableReducerJob("wc", HBaseReduceClass.class, job);
		
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", Infos.ZK_SERVICE_PORT);
		configuration.set("hbase.zookeeper.quorum",Infos.ZK_NODES);
		configuration.set("hbase.master",Infos.HBASE_MASTER_ADDR);
		configuration.set("mapred.job.tracker", "hbase:9001");
		String[] params = {"hdfs://hbase:9000/test/wc"};
		
		int result = ToolRunner.run(configuration, new HBaseHadoopReduce(), params);
		System.exit(result);

	}
}
