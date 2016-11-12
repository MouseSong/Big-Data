package learn.spark.kvopt;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 对kvRDD的转化.
 * @author zhangdong
 * @createtime 2016-11-11
 * @location peking
 * 
 * 聚合  分组  链接  排序
 * 
 * reduceByKey
 * groupByKey
 * combineByKey
 * mapValues
 * flatMapValues
 * keys
 * values
 * sortByKey
 * subtractByKey
 * join
 * rightOuterJoin
 * leftOuterJoin
 * */
public class KVRDDTrans {

	public static void main(String[] args) {
		
		SparkConf config = new SparkConf().setAppName("WordCount").setMaster("spark://hbase:7077");
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<String> lines = context.textFile("E:\\New_WorkSpace\\eclipse\\bigdata_framework_spark\\files\\1.txt");//context.parallelize(Arrays.asList("1 2", "3 4", "3 6"));
		
		JavaPairRDD<Integer, Integer> prdd1 = lines.mapToPair(
				new PairFunction<String,Integer, Integer>(){
					@Override
					public Tuple2<Integer, Integer> call(String arg0) throws Exception {
                        Integer x = Integer.parseInt(arg0.split(" ")[0]);
                        Integer y = Integer.parseInt(arg0.split(" ")[1]);
						return new Tuple2<Integer, Integer>(x, y);
					}
		});
		
		prdd1.collect().forEach(item -> {
			System.out.println(item._1 + "," + item._2);
		});
		
		
		//Wordcount
		JavaRDD<String> wcs = context.textFile("E:\\New_WorkSpace\\eclipse\\bigdata_framework_spark\\files\\1.txt");//context.parallelize(Arrays.asList("1 2", "3 4", "3 6"));
		JavaRDD<String> words = wcs.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterable<String> call(String arg0) throws Exception {
				return Arrays.asList(arg0.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> result = words.mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(arg0, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>(){
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0 + arg1;
			}
		});
		
		System.out.println(StringUtils.join(result.collect(), ","));
		
		
		
		//排序
		JavaRDD<String> sortrdd = context.parallelize(Arrays.asList("4 zhangsan", "7 lisi", "5 wangwu"));
		JavaPairRDD<Integer, String> sortprdd = sortrdd.mapToPair(new PairFunction<String, Integer, String>(){

			@Override
			public Tuple2<Integer, String> call(String arg0) throws Exception {
				String[] temp = arg0.split(" ");
				return new Tuple2<Integer, String>(Integer.valueOf(temp[0]), temp[1]);
			}
		});
		
		sortprdd.sortByKey(false);  //DESC
	}
}
