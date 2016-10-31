package learn.hbase.inaction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import learn.hbase.inaction.Infos;

public class HBaseHadoopMap extends Configured implements Tool{
	
	private static class HBaseMap extends TableMapper<Text, IntWritable>{

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			 Text outKey = new Text();
			 IntWritable outValue = new IntWritable();
			 outKey.set(value.list().get(0).getValue());
             outValue.set(1);
             context.write(outKey, outValue);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf(),"HBaseMapReduce");
		Scan scan = new Scan();
		scan.addColumn("twits".getBytes(), "twit".getBytes());
		TableMapReduceUtil.initTableMapperJob("twits", scan, HBaseMap.class, Text.class, IntWritable.class, job);
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
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
        "hdfs://hbase:9000/test/twits/out/2"};
		
		int result = ToolRunner.run(configuration, new HBaseHadoopMap(), params);
		System.exit(result);
	}
}
