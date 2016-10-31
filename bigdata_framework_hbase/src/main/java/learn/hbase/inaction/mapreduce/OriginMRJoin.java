package learn.hbase.inaction.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * implement join by origin map reduce.
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
public class OriginMRJoin extends Configured implements Tool{

	private static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String record = "";
			String[] infos = value.toString().split(" ");
			if(Objects.nonNull(infos) && infos.length == 4){
				record = "twitcount," + infos[3];
			}else if(Objects.nonNull(infos) && infos.length == 2){
				record = "spendtime," + infos[1];
			}
			if(!"".equals(record)){
				context.write(new Text(infos[0]), new Text(record));
			}
		}
	}
	
	
	private static class ReduceClass extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String> twitcounts = new ArrayList<String>();
			List<String> spendtimes = new ArrayList<String>();
			for(Text value : values){
            	String[] record = value.toString().split(",");
            	if("twitcount".equals(record[0])){
            		twitcounts.add(record[1]);
            	}else{
            		spendtimes.add(record[1]);
            	}
            }
			
			twitcounts.forEach(twitcount -> {
				spendtimes.forEach(spendtime -> {
					try {
						context.write(new Text(twitcount), new Text(spendtime));
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			});
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf(),"OriginMRJoin");
		
		job.setJarByClass(OriginMRJoin.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        String[] arguments = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
		
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
		String[] params = {"hdfs://hbase:9000/test/origin/in", "hdfs://hbase:9000/test/output/1"};
		
		int result = ToolRunner.run(configuration, new OriginMRJoin(), params);
		System.exit(result);
	}
	
}
