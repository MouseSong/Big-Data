package learn.hadoop.inaction.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
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
 * 计算各个部门的总工资.
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
public class SumSalaryOfDept extends Configured implements Tool{
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

		//缓存部门信息
		private Map<String, String> deptInfo = new HashMap<String, String>();
		//员工信息
		private String[] empInfo;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			BufferedReader reader = null;
			try{
				//在window下运行MapReduce时，由于盘符与Linux不兼容，使用getLocalCacheFiles会报错
				
				/*Path path = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
			    String deptIdName = null;
			    reader = new BufferedReader(new FileReader(path.toString()));
			    while((deptIdName = reader.readLine()) != null){
			    	deptInfo.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
			    }*/
				String deptIdName = null;
				URI[] cacheFile= DistributedCache.getCacheFiles(context.getConfiguration());
				FileSystem fs = FileSystem.get(cacheFile[0], context.getConfiguration());
				reader =  new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFile[0]))));
				while((deptIdName = reader.readLine()) != null){
			    	deptInfo.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
			    }
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				try{
					if(reader != null){
						reader.close();
					}
				}catch(IOException e){
					e.printStackTrace();
				}
			}
			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			empInfo = value.toString().split(",");
			//把部门编号join成部门名称
			if(deptInfo.containsKey(empInfo[7])){
				if(null != empInfo[5] && !"".equals(empInfo[5].toString())){
					context.write(new Text(deptInfo.get(empInfo[7])), new Text(empInfo[5]));
				}
			}
		}
	}
	
	
	//Reduce
	private static class ReduceClass extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				   Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
            long sumSalary = 0;
            for(Text salary : values){
            	sumSalary += Long.parseLong(salary.toString());
            }
		    context.write(key, new Text(String.valueOf(sumSalary)));
		}
	}
	

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = new Job(getConf(), "SumSalaryOfDept");
		job.setJarByClass(SumSalaryOfDept.class);
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
        "hdfs://node1:9000/inaction/home_task1/out/1"};
		Configuration config = new Configuration();
		config.addResource("hadoop-config.xml");
		int result = ToolRunner.run(config, new SumSalaryOfDept(), params);
		System.exit(result);
	}

	
}
