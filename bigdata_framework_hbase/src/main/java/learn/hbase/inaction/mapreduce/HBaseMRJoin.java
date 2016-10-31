package learn.hbase.inaction.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.util.Tool;



/**
 * implement join by map reduce on hbase.
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
public class HBaseMRJoin extends Configured implements Tool{

	//split datas from hbase
	@SuppressWarnings("unused")
	private static class SpendTimeMapClass extends TableMapper<Text, Text>{

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//TableMapReduceUtil.
			List<KeyValue> kvs = value.list();
			for(KeyValue kv : kvs){
			   
			}
		}
	}
	
	
	private static class TwitCountMapClass extends TableMapper<Text, Text>{

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
		}
		
	}
	
	//write result to hbase
	private static class ReduceClass extends TableReducer<Text, Text, ImmutableBytesWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
            
		}
		
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
