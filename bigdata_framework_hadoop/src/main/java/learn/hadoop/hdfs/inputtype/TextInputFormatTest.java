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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TextInputFormatTest extends Configured implements Tool{

	
	
	public int run(String[] args) throws Exception {
		
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "TextInputFormatText");
		
		job.setJarByClass(TextInputFormatTest.class);
		job.setMapperClass(MapClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
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
		args[0] = "hdfs://master:9000/typetest";
		args[1] = "hdfs://master:9000/typetestout1";
		
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9000");
		int result = ToolRunner.run(conf, new TextInputFormatTest(), args);
		
		System.exit(result);
	}
}

class MapClass extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.get() + "  " + value.toString());
		/*
		 *   0  aaaaaaaaaaaaaaaaaaaa
			21  bbbbbbbbbbbbbbbbbbb
			41  cccccccccccccccc
			58  ddddddddddddddddd
		 * */
		context.write(new Text(UUID.randomUUID()+ ""), new Text(value.toString()));
	}
	
}