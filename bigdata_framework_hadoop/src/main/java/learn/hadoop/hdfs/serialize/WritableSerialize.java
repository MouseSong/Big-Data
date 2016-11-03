package learn.hadoop.hdfs.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;


public class WritableSerialize {

	
	
	/**
	 * 序列化Writable
	 * */
	//@Test
	public void test1() throws Exception{
		IntWritable iw = new IntWritable(10);
	
		//用b来捕获  iw被序列化后的字节
	    ByteArrayOutputStream b = new ByteArrayOutputStream();
	    
	    DataOutputStream out = new DataOutputStream(b);
	    iw.write(out);
	    out.close();
	    
	    System.out.println(b.toByteArray().length);   //4
	}
	
	private byte[] serialize(Writable w) throws Exception{
		ByteArrayOutputStream b = new ByteArrayOutputStream();
	    DataOutputStream out = new DataOutputStream(b);
	    w.write(out);
	    return b.toByteArray();
	}
	/**
	 * 反序列化Writable
	 * */
	//@Test
	public void test2() throws Exception{
		
		IntWritable iw = new IntWritable(10);
		//用b来捕获  iw被序列化后的字节
	    ByteArrayOutputStream b = new ByteArrayOutputStream();
	    DataOutputStream out = new DataOutputStream(b);
	    iw.write(out);
	    out.close();
	    
	    System.out.println(b.toByteArray().length);  //4
		//用in设置需要反序列化的自己数组
		ByteArrayInputStream in = new ByteArrayInputStream(b.toByteArray());
		DataInputStream ins = new DataInputStream(in);
		
		IntWritable iw2 = new IntWritable();
		//读取流中的字节，进行反序列化
		iw2.readFields(ins);
		ins.close();
		System.out.println(iw2.get());   //10
	}
	
	
	//----------------------------------------------------
	
	/**
	 * Writable Comparable 比较对Writable来说很重要
	 * 
	 * WritableComparable<T> extends Writable, Comparable<T>
	 * 
	 * RowComparator 可以比较两个未被反序列化的数据的大小
	 * */
	@Test
	@SuppressWarnings("unchecked")
	public void test3() throws Exception {
		
		RawComparator<IntWritable> comp = WritableComparator.get(IntWritable.class);
		
		IntWritable i1 = new IntWritable(1);
		IntWritable i2 = new IntWritable(2);
		
		System.out.println(comp.compare(i1, i2));  //-1
		
		byte[] b1 = serialize(i1);
		byte[] b2 = serialize(i2);
		
		System.out.println(comp.compare(b1, 0, b1.length, b2, 0, b2.length)); // - 1
	}
	
	
	/**
	 * ArrayWritable TwoDArrayWritable
	 * 
	 * set/get
	 * */
	public void test4(){
		
		ArrayWritable a1 = new ArrayWritable(IntWritable.class);
		IntWritable[] is = new IntWritable[10];
		is[1] = new IntWritable(3);
	    a1.set(is);
	    a1.get();
	    
	    
	    TwoDArrayWritable tw = new TwoDArrayWritable(LongWritable.class);
	    LongWritable[][] ts = new LongWritable[1][1];
	    tw.set(ts);
	    
	    tw.get();
	}
}
