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

public class NLineInputFormatTest extends Configured implements Tool{

	
	
	public int run(String[] args) throws Exception {
		
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "TextInputFormatText");
		
		job.setJarByClass(NLineInputFormatTest.class);
		job.setMapperClass(NLineMapClass.class);
		
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
		args[0] = "hdfs://master:9000/type3";
		args[1] = "hdfs://master:9000/typetestout5";
		
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9000");
		conf.set("mapred.line.input.format.linespermap", "20");
		int result = ToolRunner.run(conf, new NLineInputFormatTest(), args);
		
		System.exit(result);
	}
}

class NLineMapClass extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println(key.get() + "  " + value.toString());
		int counter = context.getConfiguration().getInt("counter", 0);
		counter += 1;
		context.getConfiguration().set("counter", counter + "");
		context.write(new Text(UUID.randomUUID()+ ""), new Text(value.toString()));
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println(context.getConfiguration().getInt("counter", 0));
		super.cleanup(context);
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		context.getConfiguration().set("counter", "0");
		super.setup(context);
	}
	
	
}