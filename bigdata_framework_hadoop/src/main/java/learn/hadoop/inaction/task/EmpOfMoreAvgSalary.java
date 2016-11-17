package learn.hadoop.inaction.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 计算比平均工资高的员工的姓名和工资
 * 输入：
 *  dept.txt
 *  10,ACCOUNTING,NEW YORK
	20,RESEARCH,DALLAS
	30,SALES,CHICAGO
	40,OPERATIONS,BOSTON
	
	employee.txt
	7369,SMITH,CLERK,7902,17-12月-80,800,,20
	7499,ALLEN,SALESMAN,7698,20-2月-81,1600,300,30
	7521,WARD,SALESMAN,7698,22-2月-81,1250,500,30
	7566,JONES,MANAGER,7839,02-4月-81,2975,,20
	7654,MARTIN,SALESMAN,7698,28-9月-81,1250,1400,30
	7698,BLAKE,MANAGER,7839,01-5月-81,2850,,30
	7782,CLARK,MANAGER,7839,09-6月-81,2450,,10
	7839,KING,PRESIDENT,,17-11月-81,5000,,10
	7844,TURNER,SALESMAN,7698,08-9月-81,1500,0,30
	7900,JAMES,CLERK,7698,03-12月-81,950,,30
	7902,FORD,ANALYST,7566,03-12月-81,3000,,20
	7934,MILLER,CLERK,7782,23-1月-82,1300,,10
	
	由于有一个大表 ，一个小表，所以使用DistrubutedCache + Map sid join
 * */
public class EmpOfMoreAvgSalary extends Configured implements Tool{
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		//员工信息
		private String[] empInfo;
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			empInfo = value.toString().split(",");
			//把部门编号join成部门名称
			context.write(new Text("0"), new Text(empInfo[1] +"," + empInfo[5]));
		}
	}
	
	
	//Reduce
	private static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				   Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<Long> salarys = new ArrayList<Long>();
		    Map<String, Long> empInfos = new HashMap<String, Long>();
		    for(Text value : values){
		    	String[] temp  = value.toString().split(",");
		    	salarys.add(Long.parseLong(temp[1]));
		    	empInfos.put(temp[0], Long.parseLong(temp[1]));
		    }
		    
		    long allSalary = 0;
		    for(Long s : salarys){
		    	allSalary += s;
		    }
		    long avgSalary = allSalary / salarys.size();
		    context.write(new Text("AvgSalar:"), new Text(avgSalary + ""));
		    for(Map.Entry<String, Long> emp : empInfos.entrySet()){
		    	if(emp.getValue() > avgSalary){
		    		context.write(new Text(emp.getKey()), new Text(emp.getValue() + ""));
		    	}
		    }
		}
	}
	

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(getConf(), "EmpOfMoreAvgSalary");
		job.setJarByClass(EmpOfMoreAvgSalary.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		//输入格式
		job.setInputFormatClass(TextInputFormat.class);
		
		//Map/Reduce输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		String[] params = {"hdfs://node1:9000/inaction/home_task1/in/dept/dept.txt",
				"hdfs://node1:9000/inaction/home_task1/in/emp", 
        "hdfs://node1:9000/inaction/home_task1/out/6"};
		Configuration config = new Configuration();
		config.addResource("hadoop-config.xml");
		int result = ToolRunner.run(config, new EmpOfMoreAvgSalary(), params);
		System.exit(result);
	}

	
}
