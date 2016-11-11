package learn.spark.demo;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

/**
 * 常用的操作。
 * 
 * 
 *转换操作
 * map/filter/flatMap/distinct
 * 
 * union/intersection/subtract/cartesian(笛卡尔积)
 * 
 *行动操作
 *reduce/fold 
 *
 *
 *RDD持久化/缓存
 *MEMORY_ONLY/MEMORY_ONLY_SEQ/
 *MEMORY_AND_DISK/
 *MEMORY_AND_DESK_SEQ/DISK_ONLY
 * 
 * */
public class CommonOpt {

	public static void main(String[] args) {
		SparkConf config = new SparkConf().setAppName("WordCount").setMaster("spark://hbase:7077");
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<Integer> lines = context.parallelize(Arrays.asList(1,2,3,4,5,6,3,4));
		
		
	 
		//map
		JavaRDD<Integer> result1 = lines.map(new Function<Integer, Integer>(){
			@Override
			public Integer call(Integer arg0) throws Exception {
				return arg0 * arg0;
			}
		});
	    
		//持久化/缓存 设置
		result1.persist(StorageLevel.MEMORY_ONLY());
		
		
		System.out.println(StringUtils.join(result1.collect(), ","));
		
		result1.unpersist();
		
		//filter
		JavaRDD<Integer> result2 = lines.filter(new Function<Integer, Boolean>(){
			@Override
			public Boolean call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0 > 2;
			}
		});
		System.out.println(StringUtils.join(result2.collect(),","));
		
		
		JavaRDD<Integer> result4 = lines.distinct();   //会触发shuffle
		System.out.println(StringUtils.join(result4.collect(), ","));
		
		
		
		//flatMap
		JavaRDD<String> lines2 = context.parallelize(Arrays.asList("aaa bbb cccc","ddd eeee ffff"));
	    JavaRDD<String> result3 = lines2.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split(" "));
			}
	    });
	    System.out.println(StringUtils.join(result3.collect(), ","));
	    
	    
	
	}
}
