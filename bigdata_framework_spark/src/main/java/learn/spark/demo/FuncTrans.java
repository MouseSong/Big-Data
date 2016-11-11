package learn.spark.demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Java Api中 函数的传递
 * Function<T,R> R call(T)
 * Function2<T1, T2, R>   R call(T1, T2)
 * FlatMapFunction<T,R> Iterable<R> call<T>
 * */
public class FuncTrans {

	public static void main(String[] args) {
        SparkConf config = new SparkConf().setAppName("WordCount").setMaster("spark://hbase:7077");
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<String> lines = context.parallelize(Arrays.asList("this", "is", "spark", "work", "count", "test"));
        
		//1.匿名内部类
		JavaRDD<String> errors = lines.filter(new Function<String, Boolean>(){
			public Boolean call(String arg0) throws Exception {
				return arg0.contains("error");
			}
		});
		
		//2.具名类对象
		JavaRDD<String> errors2 = lines.filter(new ContainsError());
		
		JavaRDD<String> errors3 = lines.filter(new Contains("error"));
		
		//3.lambda 
		JavaRDD<String> errors4 = lines.filter(line -> line.contains("error"));
	}
}
class ContainsError implements Function<String, Boolean>{
	public Boolean call(String arg0) throws Exception {
		// TODO Auto-generated method stub
		return arg0.contains("error");
	}
}

class Contains implements Function<String, Boolean>{

	private String condition;
	public Contains(String condition){this.condition = condition;}
	public Boolean call(String arg0) throws Exception {
		// TODO Auto-generated method stub
		return arg0.contains(condition);
	}
	
}