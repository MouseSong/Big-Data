package learn.hbase.inaction.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import learn.hbase.inaction.Infos;



/**
 * implement join by map reduce on hbase.
 * @author zhangdong
 * @createtime 2016-10-31
 * @location peking
 * 
 * Yvonn66 YvonneMarc Yvonn66@unmercantile.com 48
   Masan46 MasanobuOlof Masan46@acetylic.com 47
   Mario23 MarionScott Mario23@Wahima.com 56
   Rober4 RobertoJacques Rober4@slidage.com 2
   
   Yvonn66 30s
   Mario23 2s
   Rober4 6s
   Masan46 35s
   
   calculate the relationship between twitcount and time
 * */
public class HBaseMRJoin extends Configured implements Tool{

	private static class MapClass extends TableMapper<Text, Text>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			value.list().forEach(kv -> {
				try{
					if("twitcount".equals(new String(kv.getQualifier()))) {//|| "time".equals(new String(kv.getQualifier())))
						context.write(new Text(String.valueOf(key.get())), new Text("twitcount-" + String.valueOf(kv.getValue())));
				    }else if ("time".equals(new String(kv.getQualifier()))){
				    	context.write(new Text(String.valueOf(key.get())), new Text("time-" + String.valueOf(kv.getValue())));
				    }
				}catch(Exception e){
					e.printStackTrace();
				}
			});
		}
	}
	
	//write result to hbase
	private static class ReduceClass extends TableReducer<Text, Text, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			 String count = "";
			 String time  = "";
			 
			 for(Text value : values){
				 String[] info = value.toString().split("-");
				 if("time".equals(info[0])){
					 time = info[1];
				 }else{
					 count = info[1];
				 }
			 }
             
			 Put put = new Put(UUID.randomUUID().toString().getBytes());
			 put.addColumn("info".getBytes(), "time".getBytes(), time.getBytes());
			 put.addColumn("info".getBytes(), "twitcount".getBytes(), count.getBytes());
			 context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "HBaseMRJoin");
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("user", scan, MapClass.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableMapperJob("twitstime", scan, MapClass.class, Text.class, Text.class, job);
		
		TableMapReduceUtil.initTableReducerJob("percentcalculate", ReduceClass.class, job);
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setNumReduceTasks(1);
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
		String[] params = {"hdfs://master:9000/example/test4/in", 
        "hdfs://hbase:9000/test/twits/out/7"};
		
		int result = ToolRunner.run(configuration, new HBaseMRJoin(), params);
		System.exit(result);
	}

}
