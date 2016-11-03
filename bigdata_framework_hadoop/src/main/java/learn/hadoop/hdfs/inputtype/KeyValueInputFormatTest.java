package learn.hadoop.hdfs.inputtype;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeyValueInputFormatTest extends Configured implements Tool{

	
	
	public int run(String[] args) throws Exception {
		
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "TextInputFormatText");
		
		job.setJarByClass(KeyValueInputFormatTest.class);
		job.setMapperClass(KVMapClass.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		
		
		String[]  arguments = new GenericOptionsParser(job.getConfiguration(),args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
		
		job.waitForCompletion(true);
		return job.isSuccessful() ?0:1;
	}
	
	public static void main(String[] args) throws Exception{
		args = new String[2];
		args[0] = "hdfs://master:9000/type2";
		args[1] = "hdfs://master:9000/typetestout4";
		
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9000");
		int result = ToolRunner.run(conf, new KeyValueInputFormatTest(), args);
		
		System.exit(result);
	}
}

class KVMapClass extends Mapper<Text, Text, Text, Text>{

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.toString() + "  " + value.toString());
		context.write(new Text(UUID.randomUUID()+ ""), new Text(value.toString()));
	}
	
}