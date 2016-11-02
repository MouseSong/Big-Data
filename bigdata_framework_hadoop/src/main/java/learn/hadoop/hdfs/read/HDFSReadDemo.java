package learn.hadoop.hdfs.read;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;
import org.junit.Test;

public class HDFSReadDemo {

	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	//Read by URL of hdfs
	//@Test()
	public void readOne() throws Exception{
		String url = "hdfs://master:9000/weather/input/200008daily.txt";
		InputStream in = null;
		try{
			in = new URL(url).openStream();
			IOUtils.copyBytes(in, System.out, 1024);
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
    //Read by FileSystem - InputStream
	//@Test
	public void testTwo() throws Exception{
		String url = "hdfs://master:9000/weather/input/200008daily.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(url), conf);
		InputStream in = null;
		try{
			in = fs.open(new Path(url));
			IOUtils.copyBytes(in, System.out, 1024);
		}finally{
			IOUtils.closeStream(in);
		}
	}
	
	
	//Read by FileSystem - FSDataInputStream
	@Test
	public void testThree() throws Exception{
		String url = "hdfs://master:9000/weather/input/200008daily.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(url), conf);
		
		/**
		 * FSDataInputSteram is sub class of DataOutputStream,
		 * it support random access, and It implement interface Seek
		 * you can invoke seek(long index) like ByteBuffer.flip()
		 * 
		 * for more, you can also invoke read to to read random from index.
		 * */
		FSDataInputStream in = null;
		try{
			in = fs.open(new Path(url));
			IOUtils.copyBytes(in, System.out, 1024);
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 1024);
			in.seek(0);
			//in.read(b, off, len)
		}finally{
			IOUtils.closeStream(in);
		}
	}
}
