package learn.spark.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf; 
//import org.apache.spark.
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		SparkConf config = new SparkConf().setAppName("WordCount").setMaster("spark://hbase:7077");
	    
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<String> rdd = context.parallelize(Arrays.asList("this", "is", "spark", "work", "count", "test"));
		JavaRDD<String> words = rdd.flatMap( new FlatMapFunction<String, String>(){
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>(){
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2(arg0,1);
			}
		}).reduceByKey(new Function2<Integer,Integer, Integer>(){
			public Integer call(Integer x, Integer y) throws Exception {
				return x + y;
			}
		});
		
		counts.saveAsTextFile("./wc/2.txt");
	}
	
}
