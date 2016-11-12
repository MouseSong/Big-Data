package learn.spark.kvopt;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 创建 键值 RDD.
 * @author zhangdong
 * @createtime 2016-11-11
 * @location peking
 * */
public class KVOptCreate {

	public static void main(String[] args) {
		SparkConf config = new SparkConf().setAppName("WordCount").setMaster("spark://hbase:7077");
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<String> lines = context.parallelize(Arrays.asList("a b c d ", "e f g h"));
		
		PairFunction<String, String, String> pf = new PairFunction<String, String, String>(){
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0.split(" ")[0], arg0);
			}
		};
		
		JavaPairRDD<String, String> prdd = lines.mapToPair(pf);
		
		Function<Tuple2<String, String>, Boolean> ff = new Function<Tuple2<String, String>, Boolean>(){
			@Override
			public Boolean call(Tuple2<String, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		};
	    
	}
}
